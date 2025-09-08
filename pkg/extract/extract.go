package extract

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	"github.com/dag-andersen/argocd-diff-preview/pkg/argoapplication"
	argocdPkg "github.com/dag-andersen/argocd-diff-preview/pkg/argocd"
	"github.com/dag-andersen/argocd-diff-preview/pkg/git"
	"github.com/dag-andersen/argocd-diff-preview/pkg/vars"
)

// Error and timeout messages that we look for in application status
// var errorMessages = []string{
// 	"helm template .",
// 	"authentication required",
// 	"authentication failed",
// 	"path does not exist",
// 	"error converting YAML to JSON",
// 	"Unknown desc = `helm template .",
// 	"Unknown desc = `kustomize build",
// 	"Unknown desc = Unable to resolve",
// 	"is not a valid chart repository or cannot be reached",
// 	"Unknown desc = repository not found",
// 	"to a commit SHA",
// }

var timeoutMessages = []string{
	"Client.Timeout",
	"failed to get git client for repo",
	"rpc error: code = Unknown desc = Get \"https",
	"i/o timeout",
	"Could not resolve host: github.com",
	":8081: connect: connection refused",
	"Temporary failure in name resolution",
	"=git-upload-pack",
	"DeadlineExceeded",
}

// const worker count
const maxWorkers = 40

// contains a app name, source path, and extracted manifest
type ExtractedApp struct {
	Id         string
	Name       string
	SourcePath string
	Manifest   []unstructured.Unstructured
	Branch     git.BranchType
}

// CreateExtractedApp creates an ExtractedApp from an ArgoResource
func CreateExtractedApp(id string, name string, sourcePath string, manifest []unstructured.Unstructured, branch git.BranchType) ExtractedApp {
	return ExtractedApp{
		Id:         id,
		Name:       name,
		SourcePath: sourcePath,
		Manifest:   manifest,
		Branch:     branch,
	}
}

// GetResourcesFromBothBranches extracts resources from both base and target branches
// by applying their manifests to the cluster and capturing the resulting resources
func GetResourcesFromBothBranches(
	argocd *argocdPkg.ArgoCDInstallation,
	timeout uint64,
	baseApps []argoapplication.ArgoResource,
	targetApps []argoapplication.ArgoResource,
	prefix string,
	deleteAfterProcessing bool,
) ([]ExtractedApp, []ExtractedApp, time.Duration, error) {
	startTime := time.Now()

	if err := verifyNoDuplicateAppIds(baseApps); err != nil {
		return nil, nil, time.Since(startTime), err
	}

	if err := verifyNoDuplicateAppIds(targetApps); err != nil {
		return nil, nil, time.Since(startTime), err
	}

	apps := append(baseApps, targetApps...)

	log.Debug().Msg("Applied manifest for both branches")
	extractedBaseApps, extractedTargetApps, err := getResourcesFromApps(argocd, apps, timeout, prefix, deleteAfterProcessing)
	if err != nil {
		return nil, nil, time.Since(startTime), fmt.Errorf("failed to get resources: %w", err)
	}
	log.Debug().Msg("Extracted manifests for both branches")

	// Recursively process nested Applications discovered in manifests
	childBase, childTarget, err := expandNestedApplications(argocd, timeout, prefix, deleteAfterProcessing, extractedBaseApps, extractedTargetApps)
	if err != nil {
		return nil, nil, time.Since(startTime), fmt.Errorf("failed to expand nested applications: %w", err)
	}

	// Append discovered nested apps to the results
	if len(childBase) > 0 {
		extractedBaseApps = append(extractedBaseApps, childBase...)
	}
	if len(childTarget) > 0 {
		extractedTargetApps = append(extractedTargetApps, childTarget...)
	}

	return extractedBaseApps, extractedTargetApps, time.Since(startTime), nil
}

// expandNestedApplications discovers nested Argo CD Applications in already extracted manifests
// and renders them as well, recursively up to a safe depth.
func expandNestedApplications(
    argocd *argocdPkg.ArgoCDInstallation,
    timeout uint64,
    prefix string,
    deleteAfterProcessing bool,
    extractedBaseApps []ExtractedApp,
    extractedTargetApps []ExtractedApp,
) ([]ExtractedApp, []ExtractedApp, error) {
    const maxDepth = 6

    // Track which app ids per branch we've already rendered to avoid loops
    visited := make(map[string]bool)

    for _, a := range extractedBaseApps {
        visited[keyForVisited(a.Id, git.Base)] = true
    }
    for _, a := range extractedTargetApps {
        visited[keyForVisited(a.Id, git.Target)] = true
    }

    // Accumulate newly extracted apps from nested levels
    var allNewBase []ExtractedApp
    var allNewTarget []ExtractedApp

    // Seed children from current extracted manifests (with loop detection)
    baseChildren, baseLoop := findChildApplicationsFromExtractedDetectLoop(extractedBaseApps, git.Base, visited)
    targetChildren, targetLoop := findChildApplicationsFromExtractedDetectLoop(extractedTargetApps, git.Target, visited)
    if baseLoop || targetLoop {
        log.Warn().Msg("Detected nested Application loop during initial discovery; stopping nested processing and proceeding with current results")
        return allNewBase, allNewTarget, nil
    }

    depth := 0
    for (len(baseChildren) > 0 || len(targetChildren) > 0) && depth < maxDepth {
        depth++

        // Combine children for processing
        var toProcess []argoapplication.ArgoResource
        toProcess = append(toProcess, baseChildren...)
        toProcess = append(toProcess, targetChildren...)

        // Render these child apps
        newBase, newTarget, err := getResourcesFromApps(argocd, toProcess, timeout, prefix, deleteAfterProcessing)
        if err != nil {
            return nil, nil, err
        }

        // Record and accumulate
        for _, a := range newBase {
            if !visited[keyForVisited(a.Id, git.Base)] {
                visited[keyForVisited(a.Id, git.Base)] = true
                allNewBase = append(allNewBase, a)
            }
        }
        for _, a := range newTarget {
            if !visited[keyForVisited(a.Id, git.Target)] {
                visited[keyForVisited(a.Id, git.Target)] = true
                allNewTarget = append(allNewTarget, a)
            }
        }

        // Discover next layer (with loop detection)
        var loop bool
        baseChildren, loop = findChildApplicationsFromExtractedDetectLoop(newBase, git.Base, visited)
        if loop {
            log.Warn().Msg("Detected nested Application loop; stopping nested processing and proceeding with current results")
            break
        }
        targetChildren, loop = findChildApplicationsFromExtractedDetectLoop(newTarget, git.Target, visited)
        if loop {
            log.Warn().Msg("Detected nested Application loop; stopping nested processing and proceeding with current results")
            break
        }
    }

    if depth >= maxDepth && (len(baseChildren) > 0 || len(targetChildren) > 0) {
        log.Warn().Msgf("Reached max nested Application depth (%d). Some nested apps may be skipped.", maxDepth)
    }

    return allNewBase, allNewTarget, nil
}

func keyForVisited(id string, branch git.BranchType) string {
    return id + "|" + string(branch)
}

// findChildApplicationsFromExtracted finds Argo CD Applications within extracted manifests and returns them as ArgoResources
func findChildApplicationsFromExtracted(list []ExtractedApp, branch git.BranchType, visited map[string]bool) []argoapplication.ArgoResource {
    children, _ := findChildApplicationsFromExtractedDetectLoop(list, branch, visited)
    return children
}

// findChildApplicationsFromExtractedDetectLoop returns children and a bool indicating a loop was detected
func findChildApplicationsFromExtractedDetectLoop(list []ExtractedApp, branch git.BranchType, visited map[string]bool) ([]argoapplication.ArgoResource, bool) {
    var children []argoapplication.ArgoResource
    for _, ea := range list {
        // Scan each manifest for Argo CD Application
        for _, obj := range ea.Manifest {
            if strings.TrimSpace(obj.GetKind()) != "Application" {
                continue
            }
            apiVersion := obj.GetAPIVersion()
            if apiVersion == "" || !strings.Contains(strings.ToLower(apiVersion), "argoproj.io/") {
                continue
            }
            name := obj.GetName()
            if strings.TrimSpace(name) == "" {
                continue
            }
            if visited[keyForVisited(name, branch)] {
                // Loop detected: already processed application referenced again as a child
                log.Warn().Str("app", name).Str("branch", string(branch)).Msg("Nested Application loop detected")
                return nil, true
            }
            copy := obj.DeepCopy()
            child := argoapplication.NewArgoResource(copy, argoapplication.Application, name, name, "embedded", branch)
            children = append(children, *child)
        }
    }
    return children, false
}

func verifyNoDuplicateAppIds(apps []argoapplication.ArgoResource) error {
	appNames := make(map[string]bool)
	for _, app := range apps {
		if appNames[app.Id] {
			return fmt.Errorf("duplicate app name: %s", app.Id)
		}
		appNames[app.Id] = true
	}
	return nil
}

// getResourcesFromApps extracts resources from Argo CD for a specific branch as ExtractedApp structs
func getResourcesFromApps(
	argocd *argocdPkg.ArgoCDInstallation,
	apps []argoapplication.ArgoResource,
	timeout uint64,
	prefix string,
	deleteAfterProcessing bool,
) ([]ExtractedApp, []ExtractedApp, error) {
	startTime := time.Now()

	log.Info().Msg("🤖 Getting Applications")

	// Process apps in parallel with a worker pool
	results := make(chan struct {
		app ExtractedApp
		err error
	}, len(apps))

	// Create a semaphore channel to limit concurrent workers
	sem := make(chan struct{}, maxWorkers)

	// Use WaitGroup to wait for all goroutines to complete (including deletions)
	var wg sync.WaitGroup

	for _, app := range apps {
		sem <- struct{}{} // Acquire semaphore
		wg.Add(1)         // Add to wait group
		go func(app argoapplication.ArgoResource) {
			defer wg.Done() // Signal completion when goroutine ends
			result, k8sName, err := getResourcesFromApp(argocd, app, timeout, prefix)
			results <- struct {
				app ExtractedApp
				err error
			}{app: result, err: err}

			// release semaphore
			<-sem

			if deleteAfterProcessing {
				// Delete Application from cluster
				log.Debug().Str("App", app.GetLongName()).Msg("Deleting application from cluster")
				if err := argocd.K8sClient.DeleteArgoCDApplication(argocd.Namespace, k8sName); err != nil {
					log.Error().Err(err).Str("App", app.GetLongName()).Msg("⚠️ Failed to delete application from cluster")
				} else {
					log.Debug().Str("App", app.GetLongName()).Msg("Deleted application from cluster")
				}
			}
		}(app)
	}

	// Collect results
	extractedBaseApps := make([]ExtractedApp, 0, len(apps))
	extractedTargetApps := make([]ExtractedApp, 0, len(apps))
	var firstError error

	// Setup progress tracking
	totalApps := len(apps)
	renderedApps := 0
	progressDone := make(chan bool)

	// Start progress reporting goroutine
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				remainingTimeSeconds := max(0, int(timeout)-int(time.Since(startTime).Seconds()))
				log.Info().Msgf("🤖 Rendered %d out of %d applications (timeout in %d seconds)", renderedApps, totalApps, remainingTimeSeconds)
			case <-progressDone:
				return
			}
		}
	}()

	for i := 0; i < len(apps); i++ {
		result := <-results
		if result.err != nil {
			if firstError == nil {
				firstError = result.err
			}
			log.Error().Err(result.err).Msg("Failed to extract app:")
			continue
		}
		switch result.app.Branch {
		case git.Base:
			extractedBaseApps = append(extractedBaseApps, result.app)
		case git.Target:
			extractedTargetApps = append(extractedTargetApps, result.app)
		default:
			return nil, nil, fmt.Errorf("unknown branch type: '%s'", result.app.Branch)
		}
		renderedApps++
	}

	// Signal progress reporting to stop
	close(progressDone)

	if firstError != nil {
		return nil, nil, firstError
	}

	// Wait for all goroutines to complete (including deletions)
	log.Info().Msg("🧼 Waiting for all application deletions to complete...")
	wg.Wait()
	log.Info().Msg("🧼 All application deletions completed")

	duration := time.Since(startTime)
	log.Info().Msgf("🤖 Got all resources from %d applications from %s-branch and got %d from %s-branch in %s", len(extractedBaseApps), git.Base, len(extractedTargetApps), git.Target, duration.Round(time.Second))

	return extractedBaseApps, extractedTargetApps, nil
}

// getResourcesFromApp extracts a single application from the cluster
// returns the extracted app, the k8s resource name, and an error
func getResourcesFromApp(argocd *argocdPkg.ArgoCDInstallation, app argoapplication.ArgoResource, timeout uint64, prefix string) (ExtractedApp, string, error) {
	// Apply the application manifest first

	err := addApplicationPrefix(&app, prefix)
	if err != nil {
		return ExtractedApp{}, "", fmt.Errorf("failed to prefix application name with prefix: %w", err)
	}

	err = labelApplicationWithRunID(&app, prefix)
	if err != nil {
		return ExtractedApp{}, "", fmt.Errorf("failed to label application with run ID: %w", err)
	}

	if err := argocd.K8sClient.ApplyManifest(app.Yaml, "string", argocd.Namespace); err != nil {
		return ExtractedApp{}, "", fmt.Errorf("failed to apply manifest for application %s: %w", app.GetLongName(), err)
	}

	log.Debug().Str("App", app.GetLongName()).Msg("Applied manifest for application")

	startTime := time.Now()
	var result ExtractedApp

	for {
		// Check if we've exceeded timeout
		if time.Since(startTime).Seconds() > float64(timeout) {
			return result, "", fmt.Errorf("timed out waiting for application %s", app.GetLongName())
		}

		// Get application status
		output, err := argocd.K8sClient.GetArgoCDApplication(argocd.Namespace, app.Id)
		if err != nil {
			return result, "", fmt.Errorf("failed to get application %s: %w", app.GetLongName(), err)
		}

		var appStatus struct {
			Status struct {
				Sync struct {
					Status string `yaml:"status"`
				} `yaml:"sync"`
				Conditions []struct {
					Type    string `yaml:"type"`
					Message string `yaml:"message"`
				} `yaml:"conditions"`
			} `yaml:"status"`
		}

		if err := yaml.Unmarshal([]byte(output), &appStatus); err != nil {
			return result, "", fmt.Errorf("failed to parse application yaml for %s: %w", app.GetLongName(), err)
		}

		switch appStatus.Status.Sync.Status {
		case "OutOfSync", "Synced":
			log.Debug().Str("App", app.GetLongName()).Msg("Extracting manifests from Application")

			retryCount := 5
			manifests, exists, err := argocd.GetManifestsWithRetry(app.Id, retryCount)
			if !exists {
				return result, "", fmt.Errorf("application %s does not exist", app.GetLongName())
			}

			if err != nil {
				return result, "", fmt.Errorf("failed to get manifests for application %s: %w", app.GetLongName(), err)
			}

			log.Debug().Str("App", app.GetLongName()).Msg("Extracted manifests from Application")

			manifests = strings.ReplaceAll(manifests, app.Id, app.Name)
			manifestsContent, err := processYamlOutput(manifests)
			if err != nil {
				log.Error().Err(err).Str("App", app.GetLongName()).Msg("Failed to process YAML")
				return result, "", fmt.Errorf("failed to process YAML: %w", err)
			}

			// Apply Application-level ignoreDifferences (jsonPointers) before comparing diffs
			rules := parseIgnoreDifferencesFromApp(app)
			if len(rules) > 0 {
				applyIgnoreDifferencesToManifests(manifestsContent, rules)
			}
			
			err = removeArgoCDTrackingID(manifestsContent)
			if err != nil {
				return result, "", fmt.Errorf("failed to remove Argo CD tracking ID: %w", err)
			}

			// remove the prefix from the application name
			oldName, err := removeApplicationPrefix(&app, prefix)
			if err != nil {
				return result, "", fmt.Errorf("failed to remove application prefix: %w", err)
			}

			// Parse the first non-empty manifest from the string
			extractedApp := CreateExtractedApp(app.Id, app.Name, app.FileName, manifestsContent, app.Branch)

			return extractedApp, oldName, nil

		case "Unknown":
			for _, condition := range appStatus.Status.Conditions {
				if isErrorCondition(condition.Type) {
					msg := condition.Message
					if containsAny(msg, timeoutMessages) {
						log.Warn().Str("App", app.GetLongName()).Msgf("⚠️ Application timed out with error: %s", msg)
						if err := argocd.RefreshApp(app.Id); err != nil {
							log.Error().Err(err).Str("App", app.GetLongName()).Msg("⚠️ Failed to refresh application")
						} else {
							log.Info().Str("App", app.GetLongName()).Msg("🔄 Refreshed application")
						}
					} else {
						log.Error().Str("App", app.GetLongName()).Msgf("❌ Application failed with error: %s", msg)
						return result, "", fmt.Errorf("application %s failed: %s", app.Name, msg)
					}
				}
			}
		}

		// Sleep before next iteration
		time.Sleep(5 * time.Second)
	}
}

func labelApplicationWithRunID(a *argoapplication.ArgoResource, runID string) error {
	labels := a.Yaml.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[vars.ArgoCDApplicationLabelKey] = runID
	a.Yaml.SetLabels(labels)
	return nil
}

// removeArgoCDTrackingID removes the "argocd.argoproj.io/tracking-id" annotation from the application
func removeArgoCDTrackingID(a []unstructured.Unstructured) error {
	for _, obj := range a {
		annotations := obj.GetAnnotations()
		if annotations == nil {
			continue
		}

		for key := range annotations {
			if key == "argocd.argoproj.io/tracking-id" {
				delete(annotations, key)
			}
		}

		obj.SetAnnotations(annotations)
	}

	return nil
}

func isErrorCondition(condType string) bool {
	return condType != "" && containsIgnoreCase(condType, "error")
}

func containsAny(s string, substrs []string) bool {
	for _, substr := range substrs {
		if s != "" && strings.Contains(s, substr) {
			return true
		}
	}
	return false
}

func containsIgnoreCase(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
