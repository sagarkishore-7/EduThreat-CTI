"""
ProtonVPN integration for IP rotation during web scraping.

Provides automatic IP rotation by connecting to different ProtonVPN servers.
Requires ProtonVPN CLI to be installed and authenticated.

Usage:
    from src.edu_cti.core.vpn import ProtonVPNManager
    
    vpn = ProtonVPNManager()
    vpn.connect()  # Connect to fastest server
    vpn.rotate()   # Connect to different server
    vpn.disconnect()
"""

import os
import subprocess
import time
import random
import logging
from typing import Optional, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class VPNStatus:
    """VPN connection status."""
    connected: bool
    server: Optional[str] = None
    country: Optional[str] = None
    ip: Optional[str] = None
    error: Optional[str] = None


# Popular server countries for web scraping (good speeds, stable connections)
PREFERRED_COUNTRIES = [
    "US",  # United States
    "GB",  # United Kingdom
    "DE",  # Germany
    "NL",  # Netherlands
    "CH",  # Switzerland
    "CA",  # Canada
    "SE",  # Sweden
    "FR",  # France
    "JP",  # Japan
    "SG",  # Singapore
]


class ProtonVPNManager:
    """
    Manager for ProtonVPN connections with IP rotation support.
    
    Requires ProtonVPN CLI to be installed and logged in:
    - macOS: brew install protonvpn-cli
    - Linux: See https://protonvpn.com/support/linux-vpn-tool/
    
    Login first: protonvpn-cli login <username>
    """
    
    def __init__(
        self,
        preferred_countries: Optional[List[str]] = None,
        rotation_interval_requests: int = 50,
        cooldown_seconds: float = 5.0,
        auto_rotate: bool = False,
    ):
        """
        Initialize VPN manager.
        
        Args:
            preferred_countries: List of country codes to rotate through (e.g., ["US", "GB"])
            rotation_interval_requests: Rotate IP every N requests (if auto_rotate)
            cooldown_seconds: Wait time after connecting before requests
            auto_rotate: Automatically rotate IP after interval
        """
        self.preferred_countries = preferred_countries or PREFERRED_COUNTRIES
        self.rotation_interval_requests = rotation_interval_requests
        self.cooldown_seconds = cooldown_seconds
        self.auto_rotate = auto_rotate
        
        self._request_count = 0
        self._current_country_idx = 0
        self._connected = False
    
    def _run_command(self, args: List[str], timeout: int = 60) -> tuple[int, str, str]:
        """Run ProtonVPN CLI command."""
        try:
            result = subprocess.run(
                ["protonvpn-cli"] + args,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return result.returncode, result.stdout, result.stderr
        except FileNotFoundError:
            return -1, "", "ProtonVPN CLI not found. Install: brew install protonvpn-cli (macOS)"
        except subprocess.TimeoutExpired:
            return -1, "", f"Command timed out after {timeout}s"
        except Exception as e:
            return -1, "", str(e)
    
    def is_available(self) -> bool:
        """Check if ProtonVPN CLI is available and logged in."""
        code, stdout, stderr = self._run_command(["status"])
        if code == -1:
            logger.warning(f"ProtonVPN not available: {stderr}")
            return False
        # Check if logged in (status command should work even if disconnected)
        return "not logged in" not in stdout.lower() and "login" not in stderr.lower()
    
    def get_status(self) -> VPNStatus:
        """Get current VPN connection status."""
        code, stdout, stderr = self._run_command(["status"])
        
        if code != 0:
            return VPNStatus(connected=False, error=stderr or "Failed to get status")
        
        # Parse status output
        # ProtonVPN CLI output format varies, try to parse common patterns
        connected = "connected" in stdout.lower() and "disconnected" not in stdout.lower()
        server = None
        country = None
        ip = None
        
        for line in stdout.splitlines():
            line = line.strip()
            if "server:" in line.lower() or "connected to:" in line.lower():
                parts = line.split(":", 1)
                if len(parts) > 1:
                    server = parts[1].strip()
            elif "country:" in line.lower():
                parts = line.split(":", 1)
                if len(parts) > 1:
                    country = parts[1].strip()
            elif "ip:" in line.lower() or "your ip:" in line.lower():
                parts = line.split(":", 1)
                if len(parts) > 1:
                    ip = parts[1].strip()
        
        self._connected = connected
        return VPNStatus(
            connected=connected,
            server=server,
            country=country,
            ip=ip,
        )
    
    def connect(self, country: Optional[str] = None) -> VPNStatus:
        """
        Connect to ProtonVPN.
        
        Args:
            country: Specific country code to connect to (e.g., "US")
                    If None, connects to fastest available server
        
        Returns:
            VPNStatus with connection result
        """
        # Disconnect first if connected
        if self._connected:
            self.disconnect()
        
        args = ["connect"]
        if country:
            args.extend(["--cc", country])
        else:
            args.append("--fastest")
        
        logger.info(f"Connecting to ProtonVPN{f' ({country})' if country else ' (fastest)'}...")
        code, stdout, stderr = self._run_command(args, timeout=30)
        
        if code != 0:
            error = stderr or stdout or "Connection failed"
            logger.error(f"ProtonVPN connection failed: {error}")
            return VPNStatus(connected=False, error=error)
        
        # Wait for connection to stabilize
        time.sleep(self.cooldown_seconds)
        
        # Get and return status
        status = self.get_status()
        if status.connected:
            logger.info(f"Connected to ProtonVPN: {status.server} ({status.country}), IP: {status.ip}")
        else:
            logger.warning("Connection command succeeded but status shows disconnected")
        
        return status
    
    def disconnect(self) -> bool:
        """Disconnect from ProtonVPN."""
        logger.info("Disconnecting from ProtonVPN...")
        code, stdout, stderr = self._run_command(["disconnect"], timeout=15)
        
        if code == 0 or "not connected" in stdout.lower():
            self._connected = False
            logger.info("Disconnected from ProtonVPN")
            return True
        
        logger.error(f"Failed to disconnect: {stderr or stdout}")
        return False
    
    def rotate(self) -> VPNStatus:
        """
        Rotate to a different VPN server.
        
        Cycles through preferred countries for variety.
        """
        # Pick next country in rotation
        country = self.preferred_countries[self._current_country_idx]
        self._current_country_idx = (self._current_country_idx + 1) % len(self.preferred_countries)
        
        logger.info(f"Rotating VPN to: {country}")
        return self.connect(country=country)
    
    def rotate_random(self) -> VPNStatus:
        """Rotate to a random VPN server from preferred countries."""
        country = random.choice(self.preferred_countries)
        logger.info(f"Rotating VPN randomly to: {country}")
        return self.connect(country=country)
    
    def on_request(self) -> None:
        """
        Called after each HTTP request.
        
        If auto_rotate is enabled, rotates IP after reaching interval.
        """
        self._request_count += 1
        
        if self.auto_rotate and self._request_count >= self.rotation_interval_requests:
            logger.info(f"Auto-rotating VPN after {self._request_count} requests")
            self.rotate()
            self._request_count = 0
    
    def __enter__(self):
        """Context manager entry - connect to VPN."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - disconnect from VPN."""
        self.disconnect()
        return False


# Singleton instance for easy access
_vpn_manager: Optional[ProtonVPNManager] = None


def get_vpn_manager() -> Optional[ProtonVPNManager]:
    """Get the singleton VPN manager instance."""
    global _vpn_manager
    
    # Check if VPN is enabled via environment variable
    if not os.getenv("EDUTHREAT_USE_VPN", "").lower() in ("1", "true", "yes"):
        return None
    
    if _vpn_manager is None:
        _vpn_manager = ProtonVPNManager(
            auto_rotate=os.getenv("EDUTHREAT_VPN_AUTO_ROTATE", "").lower() in ("1", "true", "yes"),
            rotation_interval_requests=int(os.getenv("EDUTHREAT_VPN_ROTATE_INTERVAL", "50")),
        )
        
        # Check if VPN is available
        if not _vpn_manager.is_available():
            logger.warning("VPN requested but not available")
            _vpn_manager = None
    
    return _vpn_manager


def setup_vpn_login(username: str) -> bool:
    """
    Setup ProtonVPN login (one-time setup).
    
    Args:
        username: ProtonVPN account username
    
    Returns:
        True if login successful
    """
    logger.info("Setting up ProtonVPN login...")
    
    # ProtonVPN login via CLI
    code, stdout, stderr = subprocess.run(
        ["protonvpn-cli", "login", username],
        capture_output=True,
        text=True,
        timeout=60,
    ).returncode, "", ""
    
    if code == 0:
        logger.info("ProtonVPN login successful")
        return True
    
    logger.error(f"ProtonVPN login failed: {stderr or stdout}")
    return False


if __name__ == "__main__":
    # Test VPN functionality
    import sys
    
    logging.basicConfig(level=logging.INFO)
    
    vpn = ProtonVPNManager()
    
    if not vpn.is_available():
        print("ProtonVPN CLI not available")
        sys.exit(1)
    
    print("Current status:")
    status = vpn.get_status()
    print(f"  Connected: {status.connected}")
    print(f"  Server: {status.server}")
    print(f"  Country: {status.country}")
    print(f"  IP: {status.ip}")
    
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "connect":
            country = sys.argv[2] if len(sys.argv) > 2 else None
            vpn.connect(country)
        elif cmd == "disconnect":
            vpn.disconnect()
        elif cmd == "rotate":
            vpn.rotate()
        elif cmd == "status":
            pass  # Already printed above
        else:
            print(f"Unknown command: {cmd}")
            print("Usage: python -m src.edu_cti.core.vpn [connect|disconnect|rotate|status]")
