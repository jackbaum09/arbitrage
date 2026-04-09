"""
Team name alias mapping for cross-platform matching.

Kalshi uses abbreviated city/team names in their question-style outcomes,
while Polymarket uses full official team names. This module provides a
lookup from Kalshi's short form to the canonical Polymarket name.
"""

from __future__ import annotations

# Kalshi short name -> Polymarket full name
# Keys are lowercase for case-insensitive matching.
TEAM_ALIASES: dict[str, str] = {
    # NFL
    "arizona": "Arizona Cardinals",
    "atlanta": "Atlanta Falcons",
    "baltimore": "Baltimore Ravens",
    "buffalo": "Buffalo Bills",
    "carolina": "Carolina Panthers",
    "chicago": "Chicago Bears",
    "cincinnati": "Cincinnati Bengals",
    "cleveland": "Cleveland Browns",
    "dallas": "Dallas Cowboys",
    "denver": "Denver Broncos",
    "detroit": "Detroit Lions",
    "green bay": "Green Bay Packers",
    "houston": "Houston Texans",
    "indianapolis": "Indianapolis Colts",
    "jacksonville": "Jacksonville Jaguars",
    "kansas city": "Kansas City Chiefs",
    "las vegas": "Las Vegas Raiders",
    "los angeles c": "Los Angeles Chargers",
    "los angeles r": "Los Angeles Rams",
    "miami": "Miami Dolphins",
    "minnesota": "Minnesota Vikings",
    "new england": "New England Patriots",
    "new orleans": "New Orleans Saints",
    "new york g": "New York Giants",
    "new york j": "New York Jets",
    "philadelphia": "Philadelphia Eagles",
    "pittsburgh": "Pittsburgh Steelers",
    "san francisco": "San Francisco 49ers",
    "seattle": "Seattle Seahawks",
    "tampa bay": "Tampa Bay Buccaneers",
    "tennessee": "Tennessee Titans",
    "washington": "Washington Commanders",

    # MLB
    "chicago c": "Chicago Cubs",
    "chicago ws": "Chicago White Sox",
    "colorado": "Colorado Rockies",
    "los angeles a": "Los Angeles Angels",
    "los angeles d": "Los Angeles Dodgers",
    "milwaukee": "Milwaukee Brewers",
    "new york m": "New York Mets",
    "new york y": "New York Yankees",
    "san diego": "San Diego Padres",
    "st. louis": "St. Louis Cardinals",
    "texas": "Texas Rangers",
    "toronto": "Toronto Blue Jays",
    "a's": "Athletics",
    # MLB teams that share city names with NFL (override by context)
    # These are handled by sport-specific lookups below

    # NHL
    "anaheim": "Anaheim Ducks",
    "boston": "Boston Bruins",
    "calgary": "Calgary Flames",
    "chicago b": "Chicago Blackhawks",
    "columbus": "Columbus Blue Jackets",
    "edmonton": "Edmonton Oilers",
    "florida": "Florida Panthers",
    "los angeles k": "Los Angeles Kings",
    "montreal": "Montreal Canadiens",
    "nashville": "Nashville Predators",
    "new jersey": "New Jersey Devils",
    "new york i": "New York Islanders",
    "new york r": "New York Rangers",
    "ottawa": "Ottawa Senators",
    "san jose": "San Jose Sharks",
    "st louis": "St. Louis Blues",
    "vancouver": "Vancouver Canucks",
    "vegas": "Vegas Golden Knights",
    "winnipeg": "Winnipeg Jets",
    "utah": "Utah Mammoth",
}

# Sport-specific overrides for ambiguous city names
# When a city name matches multiple sports, use these sport-specific maps.
SPORT_TEAM_ALIASES: dict[str, dict[str, str]] = {
    "nfl": {
        "houston": "Houston Texans",
        "miami": "Miami Dolphins",
        "arizona": "Arizona Cardinals",
        "atlanta": "Atlanta Falcons",
        "baltimore": "Baltimore Ravens",
        "pittsburgh": "Pittsburgh Steelers",
        "cleveland": "Cleveland Browns",
        "cincinnati": "Cincinnati Bengals",
        "carolina": "Carolina Panthers",
        "tampa bay": "Tampa Bay Buccaneers",
        "seattle": "Seattle Seahawks",
        "washington": "Washington Commanders",
        "minnesota": "Minnesota Vikings",
        "detroit": "Detroit Lions",
        "chicago": "Chicago Bears",
        "jacksonville": "Jacksonville Jaguars",
        "tennessee": "Tennessee Titans",
        "indianapolis": "Indianapolis Colts",
        "new orleans": "New Orleans Saints",
        "philadelphia": "Philadelphia Eagles",
        "dallas": "Dallas Cowboys",
        "denver": "Denver Broncos",
        "buffalo": "Buffalo Bills",
        "new england": "New England Patriots",
        "green bay": "Green Bay Packers",
        "kansas city": "Kansas City Chiefs",
        "las vegas": "Las Vegas Raiders",
        "san francisco": "San Francisco 49ers",
    },
    "mlb": {
        "houston": "Houston Astros",
        "miami": "Miami Marlins",
        "arizona": "Arizona Diamondbacks",
        "atlanta": "Atlanta Braves",
        "baltimore": "Baltimore Orioles",
        "pittsburgh": "Pittsburgh Pirates",
        "cleveland": "Cleveland Guardians",
        "cincinnati": "Cincinnati Reds",
        "tampa bay": "Tampa Bay Rays",
        "seattle": "Seattle Mariners",
        "washington": "Washington Nationals",
        "minnesota": "Minnesota Twins",
        "detroit": "Detroit Tigers",
        "kansas city": "Kansas City Royals",
        "boston": "Boston Red Sox",
        "philadelphia": "Philadelphia Phillies",
        "colorado": "Colorado Rockies",
        "milwaukee": "Milwaukee Brewers",
        "texas": "Texas Rangers",
        "toronto": "Toronto Blue Jays",
        "san diego": "San Diego Padres",
        "san francisco": "San Francisco Giants",
        "st. louis": "St. Louis Cardinals",
        "chicago c": "Chicago Cubs",
        "chicago ws": "Chicago White Sox",
        "los angeles a": "Los Angeles Angels",
        "los angeles d": "Los Angeles Dodgers",
        "new york m": "New York Mets",
        "new york y": "New York Yankees",
        "a's": "Athletics",
    },
    "nhl": {
        "anaheim": "Anaheim Ducks",
        "boston": "Boston Bruins",
        "buffalo": "Buffalo Sabres",
        "calgary": "Calgary Flames",
        "carolina": "Carolina Hurricanes",
        "chicago": "Chicago Blackhawks",
        "colorado": "Colorado Avalanche",
        "columbus": "Columbus Blue Jackets",
        "dallas": "Dallas Stars",
        "detroit": "Detroit Red Wings",
        "edmonton": "Edmonton Oilers",
        "florida": "Florida Panthers",
        "los angeles": "Los Angeles Kings",
        "minnesota": "Minnesota Wild",
        "montreal": "Montreal Canadiens",
        "nashville": "Nashville Predators",
        "new jersey": "New Jersey Devils",
        "new york i": "New York Islanders",
        "new york r": "New York Rangers",
        "ottawa": "Ottawa Senators",
        "philadelphia": "Philadelphia Flyers",
        "pittsburgh": "Pittsburgh Penguins",
        "san jose": "San Jose Sharks",
        "seattle": "Seattle Kraken",
        "st. louis": "St. Louis Blues",
        "st louis": "St. Louis Blues",
        "tampa bay": "Tampa Bay Lightning",
        "toronto": "Toronto Maple Leafs",
        "utah": "Utah Mammoth",
        "vancouver": "Vancouver Canucks",
        "vegas": "Vegas Golden Knights",
        "washington": "Washington Capitals",
        "winnipeg": "Winnipeg Jets",
    },
    "nba": {
        # NBA Kalshi doesn't currently use futures table, but for completeness
    },
    "cbb": {},
}


def resolve_team(kalshi_short: str, sport: str | None = None) -> str | None:
    """
    Resolve a Kalshi short team name to the full Polymarket team name.

    If sport is provided, uses sport-specific mapping to handle ambiguous
    city names (e.g., "Houston" -> Texans vs Astros).
    Falls back to the generic TEAM_ALIASES if no sport-specific match.
    """
    key = kalshi_short.strip().lower()

    # Try sport-specific first
    if sport:
        sport_map = SPORT_TEAM_ALIASES.get(sport, {})
        if key in sport_map:
            return sport_map[key]

    # Fall back to generic
    return TEAM_ALIASES.get(key)
