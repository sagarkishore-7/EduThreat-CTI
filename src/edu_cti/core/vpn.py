"""
NordVPN integration for IP rotation during web scraping.

Provides automatic IP rotation by connecting to different NordVPN servers.
Requires NordVPN CLI to be installed and authenticated.

Usage:
    from src.edu_cti.core.vpn import NordVPNManager
    
    vpn = NordVPNManager()
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
    "United_States",
    "United_Kingdom", 
    "Germany",
    "Netherlands",
    "Switzerland",
    "Canada",
    "Sweden",
    "France",
    "Japan",
    "Singapore",
]


class NordVPNManager:
    """
    Manager for NordVPN connections with IP rotation support.
    
    Requires NordVPN CLI to be installed and logged in:
    - macOS: brew install --cask nordvpn
    - Linux: Download from nordvpn.com
    
    Login first: nordvpn login
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
            preferred_countries: List of countries to rotate through
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
        """Run NordVPN CLI command."""
        try:
            result = subprocess.run(
                ["nordvpn"] + args,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return result.returncode, result.stdout, result.stderr
        except FileNotFoundError:
            return -1, "", "NordVPN CLI not found. Install from: brew install --cask nordvpn (macOS)"
        except subprocess.TimeoutExpired:
            return -1, "", f"Command timed out after {timeout}s"
        except Exception as e:
            return -1, "", str(e)
    
    def is_available(self) -> bool:
        """Check if NordVPN CLI is available and logged in."""
        code, stdout, stderr = self._run_command(["status"])
        if code == -1:
            logger.warning(f"NordVPN not available: {stderr}")
            return False
        return True
    
    def get_status(self) -> VPNStatus:
        """Get current VPN connection status."""
        code, stdout, stderr = self._run_command(["status"])
        
        if code != 0:
            return VPNStatus(connected=False, error=stderr or "Failed to get status")
        
        # Parse status output
        connected = "Connected" in stdout or "Status: Connected" in stdout
        server = None
        country = None
        ip = None
        
        for line in stdout.splitlines():
            line = line.strip()
            if line.startswith("Server:") or line.startswith("Current server:"):
                server = line.split(":", 1)[1].strip()
            elif line.startswith("Country:"):
                country = line.split(":", 1)[1].strip()
            elif line.startswith("IP:") or line.startswith("Your new IP:"):
                ip = line.split(":", 1)[1].strip()
        
        self._connected = connected
        return VPNStatus(
            connected=connected,
            server=server,
            country=country,
            ip=ip,
        )
    
    def connect(self, country: Optional[str] = None) -> VPNStatus:
        """
        Connect to NordVPN.
        
        Args:
            country: Specific country to connect to (e.g., "United_States")
                    If None, connects to fastest available server
        
        Returns:
            VPNStatus with connection result
        """
        # Disconnect first if connected
        if self._connected:
            self.disconnect()
        
        args = ["connect"]
        if country:
            args.append(country)
        
        logger.info(f"Connecting to NordVPN{f' ({country})' if country else ''}...")
        code, stdout, stderr = self._run_command(args, timeout=30)
        
        if code != 0:
            error = stderr or stdout or "Connection failed"
            logger.error(f"NordVPN connection failed: {error}")
            return VPNStatus(connected=False, error=error)
        
        # Wait for connection to stabilize
        time.sleep(self.cooldown_seconds)
        
        # Get and return status
        status = self.get_status()
        if status.connected:
            logger.info(f"Connected to NordVPN: {status.server} ({status.country}), IP: {status.ip}")
        else:
            logger.warning("Connection command succeeded but status shows disconnected")
        
        return status
    
    def disconnect(self) -> bool:
        """Disconnect from NordVPN."""
        logger.info("Disconnecting from NordVPN...")
        code, stdout, stderr = self._run_command(["disconnect"], timeout=15)
        
        if code == 0 or "not connected" in stdout.lower():
            self._connected = False
            logger.info("Disconnected from NordVPN")
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
_vpn_manager: Optional[NordVPNManager] = None


def get_vpn_manager() -> Optional[NordVPNManager]:
    """Get the singleton VPN manager instance."""
    global _vpn_manager
    
    # Check if VPN is enabled via environment variable
    if not os.getenv("EDUTHREAT_USE_VPN", "").lower() in ("1", "true", "yes"):
        return None
    
    if _vpn_manager is None:
        _vpn_manager = NordVPNManager(
            auto_rotate=os.getenv("EDUTHREAT_VPN_AUTO_ROTATE", "").lower() in ("1", "true", "yes"),
            rotation_interval_requests=int(os.getenv("EDUTHREAT_VPN_ROTATE_INTERVAL", "50")),
        )
        
        # Check if VPN is available
        if not _vpn_manager.is_available():
            logger.warning("VPN requested but not available")
            _vpn_manager = None
    
    return _vpn_manager


def setup_vpn_login(email: str, password: str) -> bool:
    """
    Setup NordVPN login (one-time setup).
    
    Note: This uses the legacy login method. Recommended to use:
    nordvpn login --token <token>
    
    Args:
        email: NordVPN account email
        password: NordVPN account password
    
    Returns:
        True if login successful
    """
    logger.info("Setting up NordVPN login...")
    
    # NordVPN login via CLI is interactive, this is a simplified version
    # In practice, use: nordvpn login --token <your-token>
    logger.warning(
        "For security, use token-based login instead:\n"
        "1. Go to https://my.nordaccount.com/dashboard/nordvpn/\n"
        "2. Generate an access token\n"
        "3. Run: nordvpn login --token <your-token>"
    )
    
    return False


if __name__ == "__main__":
    # Test VPN functionality
    import sys
    
    logging.basicConfig(level=logging.INFO)
    
    vpn = NordVPNManager()
    
    if not vpn.is_available():
        print("NordVPN CLI not available")
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
