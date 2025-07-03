from dataclasses import dataclass
from typing import List, Dict, Optional

@dataclass
class SteamAppDetails:
    title: str
    appid: int
    price: Optional[float]  # None for free games
    genres: List[str]
    short_description: str
    release_date: Optional[str]  # Now explicitly Optional
    windows: bool
    linux: bool
    mac: bool
    price_original: Optional[float]
    steam_deck: Optional[bool]

def parse_response(response: dict) -> Optional[SteamAppDetails]:
    """Parses Steam API response into SteamAppDetails dataclass"""
    if not response.get("success", False):
        return None
    
    data = response["data"]
    
    # Handle price data
    price_data = data.get("price_overview", {})
    current_price = price_data.get("final", 0) / 100 if price_data else 0.0
    original_price = price_data.get("initial", 0) / 100 if price_data else None
    
    # Handle platforms
    platforms = data.get("platforms", {})
    
    # Handle Steam Deck compatibility
    steam_deck = None
    if "steamdeck" in platforms:
        steam_deck = platforms["steamdeck"].get("supported", False)
    
    # Handle release date - returns None if not available
    release_date = data.get("release_date", {}).get("date")
    
    return SteamAppDetails(
        title=data.get("name", "Unknown"),
        appid=data.get("steam_appid", 0),
        price=current_price,
        genres=[g["description"] for g in data.get("genres", [])],
        short_description=data.get("short_description", ""),
        release_date=release_date,  # Now returns None if missing
        windows=platforms.get("windows", False),
        linux=platforms.get("linux", False),
        mac=platforms.get("mac", False),
        price_original=original_price,
        steam_deck=steam_deck
    )