"""
Fan-Out/Fan-In Pattern Example.

This example demonstrates the fan-out/fan-in orchestration pattern where:
- Fan-Out: Multiple agents process the same input context concurrently
- Fan-In: Results from all agents are aggregated into a single output context

Use Cases:
- Data enrichment from multiple sources
- Parallel API calls for aggregated responses
- Multi-model AI inference with result fusion
- Distributed data gathering and consolidation

Expected Output:
    === Fan-Out/Fan-In Pattern Example ===
    
    Scenario: User Dashboard Data Aggregation
    --------------------------------------------------
    
    1. Sequential Execution (Chaining Strategy)
       → UserProfileAgent processing...
       → PreferencesAgent processing...
       → AnalyticsAgent processing...
       → RecommendationsAgent processing...
       ✓ Sequential execution completed in 2.01s
       
    2. Parallel Execution (Fan-Out/Fan-In Strategy)
       → All agents processing in parallel...
       ✓ Parallel execution completed in 0.51s
       
    Performance Improvement: 3.94x faster
    
    3. Aggregated Results
       Keys: ['user_id', 'profile', 'preferences', 'analytics', 'recommendations']
       
    4. Error Handling Demo
       ✓ Graceful degradation when some agents fail
"""

import asyncio
import time
from typing import Any
from cmp import CMP
from cmp.sdk.agent import Agent
from cmp.core.models import Context
from cmp.orchestration.strategies import FanOutFanInStrategy, ChainingStrategy


# ============================================================================
# Agent Implementations
# ============================================================================

class UserProfileAgent(Agent):
    """Fetches user profile data (simulated)."""
    
    def __init__(self, delay: float = 0.5):
        super().__init__("user_profile_agent")
        self.delay = delay
    
    async def process(self, context: Context) -> Context:
        """Fetch user profile information."""
        print(f"   → {self.agent_id}: Fetching user profile...")
        
        # Simulate API call delay
        await asyncio.sleep(self.delay)
        
        user_id = context.data.get("user_id", "unknown")
        profile_data = {
            "profile": {
                "name": "John Doe",
                "email": f"{user_id}@example.com",
                "joined": "2024-01-15",
                "tier": "premium"
            }
        }
        
        return context.with_data(**profile_data)


class PreferencesAgent(Agent):
    """Fetches user preferences (simulated)."""
    
    def __init__(self, delay: float = 0.5):
        super().__init__("preferences_agent")
        self.delay = delay
    
    async def process(self, context: Context) -> Context:
        """Fetch user preferences."""
        print(f"   → {self.agent_id}: Fetching user preferences...")
        
        await asyncio.sleep(self.delay)
        
        preferences_data = {
            "preferences": {
                "theme": "dark",
                "language": "en",
                "notifications": True,
                "newsletter": False
            }
        }
        
        return context.with_data(**preferences_data)


class AnalyticsAgent(Agent):
    """Fetches user analytics data (simulated)."""
    
    def __init__(self, delay: float = 0.5):
        super().__init__("analytics_agent")
        self.delay = delay
    
    async def process(self, context: Context) -> Context:
        """Fetch user analytics."""
        print(f"   → {self.agent_id}: Fetching analytics data...")
        
        await asyncio.sleep(self.delay)
        
        analytics_data = {
            "analytics": {
                "page_views": 1234,
                "session_count": 45,
                "avg_session_duration": "5m 32s",
                "last_active": "2025-12-15T18:30:00Z"
            }
        }
        
        return context.with_data(**analytics_data)


class RecommendationsAgent(Agent):
    """Generates personalized recommendations (simulated)."""
    
    def __init__(self, delay: float = 0.5):
        super().__init__("recommendations_agent")
        self.delay = delay
    
    async def process(self, context: Context) -> Context:
        """Generate recommendations."""
        print(f"   → {self.agent_id}: Generating recommendations...")
        
        await asyncio.sleep(self.delay)
        
        recommendations_data = {
            "recommendations": [
                {"id": "rec_1", "title": "Complete your profile", "priority": "high"},
                {"id": "rec_2", "title": "Try new features", "priority": "medium"},
                {"id": "rec_3", "title": "Invite friends", "priority": "low"}
            ]
        }
        
        return context.with_data(**recommendations_data)


class UnreliableAgent(Agent):
    """Agent that sometimes fails (for error handling demo)."""
    
    def __init__(self, fail: bool = False):
        super().__init__("unreliable_agent")
        self.fail = fail
    
    async def process(self, context: Context) -> Context:
        """Process with potential failure."""
        print(f"   → {self.agent_id}: Processing...")
        
        if self.fail:
            print(f"   ✗ {self.agent_id}: Failed!")
            raise Exception("Simulated failure")
        
        await asyncio.sleep(0.3)
        return context.with_data(unreliable_data="success")


# ============================================================================
# Example Functions
# ============================================================================

async def sequential_execution_example(cmp: CMP, context: Context, agents: list[Agent]) -> tuple[Context, float]:
    """Execute agents sequentially using chaining strategy."""
    print("\n1. Sequential Execution (Chaining Strategy)")
    print("   " + "-" * 50)
    
    start_time = time.time()
    
    # Use chaining strategy (sequential)
    strategy = ChainingStrategy()
    result_context = context
    
    async for ctx in strategy.execute(context, agents, {}):
        result_context = ctx
    
    elapsed = time.time() - start_time
    print(f"   ✓ Sequential execution completed in {elapsed:.2f}s")
    
    return result_context, elapsed


async def parallel_execution_example(cmp: CMP, context: Context, agents: list[Agent]) -> tuple[Context, float]:
    """Execute agents in parallel using fan-out/fan-in strategy."""
    print("\n2. Parallel Execution (Fan-Out/Fan-In Strategy)")
    print("   " + "-" * 50)
    print("   → All agents processing in parallel...")
    
    start_time = time.time()
    
    # Use fan-out/fan-in strategy (parallel)
    strategy = FanOutFanInStrategy()
    result_context = context
    
    async for ctx in strategy.execute(context, agents, {}):
        result_context = ctx
    
    elapsed = time.time() - start_time
    print(f"   ✓ Parallel execution completed in {elapsed:.2f}s")
    
    return result_context, elapsed


async def error_handling_example(cmp: CMP, context: Context):
    """Demonstrate graceful error handling in fan-out/fan-in."""
    print("\n4. Error Handling Demo")
    print("   " + "-" * 50)
    print("   Testing graceful degradation when some agents fail...")
    
    # Mix of successful and failing agents
    agents = [
        UserProfileAgent(delay=0.2),
        UnreliableAgent(fail=True),  # This will fail
        PreferencesAgent(delay=0.2),
        UnreliableAgent(fail=True),  # This will also fail
    ]
    
    strategy = FanOutFanInStrategy()
    result_context = context
    
    async for ctx in strategy.execute(context, agents, {}):
        result_context = ctx
    
    # Check what data we got despite failures
    successful_keys = [k for k in result_context.data.keys() if k != "user_id"]
    print(f"   ✓ Gracefully handled failures")
    print(f"   ✓ Received data from {len(successful_keys)} successful agents: {successful_keys}")


async def main():
    """Run complete fan-out/fan-in example."""
    
    print("=" * 60)
    print("Fan-Out/Fan-In Pattern Example")
    print("=" * 60)
    print("\nScenario: User Dashboard Data Aggregation")
    print("-" * 50)
    print("Fetching user profile, preferences, analytics, and")
    print("recommendations from multiple services in parallel.")
    print("-" * 50)
    
    # Initialize CMP
    cmp = CMP(tenant_id="demo_tenant")
    
    # Create initial context
    context_id = await cmp.context() \
        .with_data({"user_id": "user_12345"}) \
        .with_schema("user_dashboard") \
        .create()
    
    # Get context
    result = await cmp.services.get_service('context_service').get(context_id)
    context = result.unwrap()
    
    # Create agents
    agents = [
        UserProfileAgent(delay=0.5),
        PreferencesAgent(delay=0.5),
        AnalyticsAgent(delay=0.5),
        RecommendationsAgent(delay=0.5),
    ]
    
    # Example 1: Sequential execution
    seq_context, seq_time = await sequential_execution_example(cmp, context, agents)
    
    # Example 2: Parallel execution
    par_context, par_time = await parallel_execution_example(cmp, context, agents)
    
    # Show performance improvement
    print(f"\n{'Performance Comparison':^60}")
    print("=" * 60)
    speedup = seq_time / par_time if par_time > 0 else 0
    print(f"   Sequential time:  {seq_time:.2f}s")
    print(f"   Parallel time:    {par_time:.2f}s")
    print(f"   Speedup:          {speedup:.2f}x faster")
    print(f"   Time saved:       {seq_time - par_time:.2f}s ({((seq_time - par_time) / seq_time * 100):.1f}%)")
    
    # Example 3: Show aggregated results
    print(f"\n3. Aggregated Results")
    print("   " + "-" * 50)
    print(f"   Total keys in result: {len(par_context.data)}")
    print(f"   Keys: {list(par_context.data.keys())}")
    print(f"\n   Sample data:")
    for key, value in par_context.data.items():
        if key != "user_id":
            if isinstance(value, dict):
                print(f"   - {key}: {len(value)} fields")
            elif isinstance(value, list):
                print(f"   - {key}: {len(value)} items")
            else:
                print(f"   - {key}: {value}")
    
    # Example 4: Error handling
    await error_handling_example(cmp, context)
    
    # Summary
    print("\n" + "=" * 60)
    print("Key Takeaways")
    print("=" * 60)
    print("✓ Fan-out/fan-in enables parallel processing")
    print("✓ Significant performance improvement for I/O-bound tasks")
    print("✓ Results are automatically aggregated")
    print("✓ Graceful error handling - partial results still returned")
    print("✓ Ideal for: API aggregation, data enrichment, multi-source queries")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
