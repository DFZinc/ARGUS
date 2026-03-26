from volume_monitor import VolumeMonitor  # import from WalletEQ

class LaunchDetector:
    def __init__(self, rate_limiter):
        self.monitor = VolumeMonitor(rate_limiter)

    async def get_new_launches(self) -> list[dict]:
        all_tokens = await self.monitor.get_active_tokens()
        # Filter to launches < 6 hours old with volume
        cutoff = datetime.now().timestamp() - (6 * 3600)
        new_launches = [t for t in all_tokens if t.get("pair_created_at", 0) > cutoff]
        return new_launches[:20]  # top 20 newest