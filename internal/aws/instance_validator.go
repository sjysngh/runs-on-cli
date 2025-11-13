package aws

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// EC2InstanceFamilyValidator validates AWS EC2 instance families using the AWS API.
// It implements the InstanceFamilyValidator interface from the runs-on/config package.
//
// This validator uses a two-tier caching strategy:
// 1. Cache of ALL available instance types for the configured region (fetched once)
// 2. Per-family cache of validation results (for fast repeat lookups)
//
// The validator is region-aware at construction time - the region from aws.Config
// is captured when NewEC2InstanceFamilyValidator is called and used for all validations.
type EC2InstanceFamilyValidator struct {
	client            *ec2.Client     // AWS EC2 client for making API calls
	region            string          // AWS region from config (immutable after construction)
	familyCache       map[string]bool // Cache: "family" -> exists (true/false)
	instanceTypes     []string        // Cache: all instance types in the region (loaded on first use)
	instanceTypesOnce sync.Once       // Ensures instance types are fetched exactly once
	mu                sync.RWMutex    // Protects familyCache from concurrent access
}

// NewEC2InstanceFamilyValidator creates a new validator that uses the AWS EC2 API.
//
// The validator captures the region from the AWS config at construction time.
// All subsequent validations will check instance families in this region.
//
// Parameters:
//   - cfg: AWS configuration (contains region, credentials, etc.)
//
// Returns:
//   - A new EC2InstanceFamilyValidator configured for cfg.Region
//
// Example:
//
//	awsConfig, _ := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
//	validator := aws.NewEC2InstanceFamilyValidator(awsConfig)
//	// All validations will check us-east-1
func NewEC2InstanceFamilyValidator(cfg aws.Config) *EC2InstanceFamilyValidator {
	return &EC2InstanceFamilyValidator{
		client:        ec2.NewFromConfig(cfg),
		region:        cfg.Region,
		familyCache:   make(map[string]bool),
		instanceTypes: nil, // Loaded lazily on first validation
	}
}

// ValidateInstanceFamily checks if an instance family exists in the validator's configured region.
//
// This method implements the InstanceFamilyValidator interface. It uses a two-tier cache:
// 1. First checks if we've already validated this family (familyCache)
// 2. If not, ensures instance types are loaded from AWS (happens once via sync.Once)
// 3. Checks if any instance type starts with the family prefix
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - family: Instance family name (e.g., "m5", "t3", "c5")
//
// Returns:
//   - bool: true if the instance family exists in the configured region, false otherwise
//   - error: any error encountered (API errors, network issues, etc.)
//
// Thread-safety: This method is safe to call from multiple goroutines.
//
// Example:
//
//	validator := aws.NewEC2InstanceFamilyValidator(awsConfig) // region from awsConfig
//	valid, err := validator.ValidateInstanceFamily(ctx, "m5")
//	if err != nil {
//	    log.Printf("API error: %v", err)
//	}
//	if !valid {
//	    log.Printf("Instance family m5 doesn't exist in %s", awsConfig.Region)
//	}
func (v *EC2InstanceFamilyValidator) ValidateInstanceFamily(ctx context.Context, family string) (bool, error) {
	// Check family cache first (read lock - allows concurrent reads)
	v.mu.RLock()
	if exists, found := v.familyCache[family]; found {
		// Cache hit! Return the cached result without calling AWS
		v.mu.RUnlock()
		return exists, nil
	}
	v.mu.RUnlock()

	// Family not in cache - ensure instance types are loaded
	// sync.Once ensures this happens exactly once, even with concurrent calls
	var loadErr error
	v.instanceTypesOnce.Do(func() {
		loadErr = v.loadInstanceTypes(ctx)
	})
	if loadErr != nil {
		return false, loadErr
	}

	// Check if any instance type starts with the family prefix
	// Example: family="t3" matches "t3.nano", "t3.micro", "t3.small", etc.
	exists := false
	familyPrefix := family + "."
	for _, instanceType := range v.instanceTypes {
		if strings.HasPrefix(instanceType, familyPrefix) {
			exists = true
			break
		}
	}

	// Cache the result for this family (write lock - exclusive access)
	v.mu.Lock()
	v.familyCache[family] = exists
	v.mu.Unlock()

	return exists, nil
}

// loadInstanceTypes fetches all available instance types for the validator's configured region.
// This method is called exactly once (via sync.Once) on the first validation.
//
// The results are stored in v.instanceTypes and used for all subsequent validations.
// This is a private helper method.
func (v *EC2InstanceFamilyValidator) loadInstanceTypes(ctx context.Context) error {
	var allInstanceTypes []string
	var nextToken *string

	// AWS API paginates results - we need to fetch all pages
	for {
		input := &ec2.DescribeInstanceTypeOfferingsInput{
			LocationType: types.LocationTypeRegion,
			Filters: []types.Filter{
				{
					Name:   aws.String("location"),
					Values: []string{v.region},
				},
			},
		}

		// If there's a next token, include it to get the next page
		if nextToken != nil {
			input.NextToken = nextToken
		}

		output, err := v.client.DescribeInstanceTypeOfferings(ctx, input)
		if err != nil {
			return fmt.Errorf("failed to fetch instance types for region %q: %w", v.region, err)
		}

		// Extract instance type names from the response
		for _, offering := range output.InstanceTypeOfferings {
			allInstanceTypes = append(allInstanceTypes, string(offering.InstanceType))
		}

		// Check if there are more pages
		if output.NextToken == nil {
			break
		}
		nextToken = output.NextToken
	}

	// Store the results in the struct (no lock needed - sync.Once guarantees single execution)
	v.instanceTypes = allInstanceTypes

	return nil
}

// ClearCache removes all cached validation results and instance type lists.
//
// This is useful for:
//   - Testing: Clear cache between test cases
//   - Force refresh: Re-fetch instance types from AWS
//
// Note: After calling ClearCache, you need to create a new validator instance
// to reload instance types, as sync.Once cannot be reset.
//
// Thread-safety: This method is safe to call from multiple goroutines.
//
// Example:
//
//	validator.ClearCache()  // Clears family cache, but instanceTypesOnce remains "done"
func (v *EC2InstanceFamilyValidator) ClearCache() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.familyCache = make(map[string]bool)
	v.instanceTypes = nil
	// Note: We cannot reset v.instanceTypesOnce - create a new validator to fully reset
}

// GetCacheStats returns statistics about the caches.
//
// Returns:
//   - familyCount: Number of family validation results cached
//   - instanceTypesLoaded: Whether instance types have been loaded from AWS
//   - familyKeys: All cached family names
//   - region: The configured region for this validator
//
// Thread-safety: This method is safe to call from multiple goroutines.
//
// Example:
//
//	familyCount, loaded, familyKeys, region := validator.GetCacheStats()
//	log.Printf("Cached %d families for region %s (types loaded: %v)", familyCount, region, loaded)
func (v *EC2InstanceFamilyValidator) GetCacheStats() (familyCount int, instanceTypesLoaded bool, familyKeys []string, region string) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	familyCount = len(v.familyCache)
	instanceTypesLoaded = v.instanceTypes != nil

	familyKeys = make([]string, 0, familyCount)
	for k := range v.familyCache {
		familyKeys = append(familyKeys, k)
	}

	return familyCount, instanceTypesLoaded, familyKeys, v.region
}
