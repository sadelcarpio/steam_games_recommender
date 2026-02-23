from enum import Enum
from typing import List, Optional
from fastapi import FastAPI
from pydantic import BaseModel, TypeAdapter
from retrieval import execute_query, semantic_search, metadata_search
from openai import OpenAI
import json

# ==========================================================
# 1. ENUMS
# ==========================================================
class Genre(str, Enum):
    NUDITY = "Nudity"
    VIOLENT = "Violent"
    GORE = "Gore"
    EDUCATION = "Education"
    ACCOUNTING = "Accounting"
    ANIMATION_MODELING = "Animation & Modeling"
    TUTORIAL = "Tutorial"
    SOFTWARE_TRAINING = "Software Training"
    STRATEGY = "Strategy"
    FREE_TO_PLAY = "Free To Play"
    DOCUMENTARY = "Documentary"
    EARLY_ACCESS = "Early Access"
    MOVIE = "Movie"
    AUDIO_PRODUCTION = "Audio Production"
    RACING = "Racing"
    PHOTO_EDITING = "Photo Editing"
    SHORT = "Short"
    UTILITIES = "Utilities"
    SPORTS = "Sports"
    CASUAL = "Casual"
    SEXUAL_CONTENT = "Sexual Content"
    DESIGN_ILLUSTRATION = "Design & Illustration"
    ADVENTURE = "Adventure"
    INDIE = "Indie"
    ACTION = "Action"
    WEB_PUBLISHING = "Web Publishing"
    SIMULATION = "Simulation"
    RPG = "RPG"
    VIDEO_PRODUCTION = "Video Production"
    GAME_DEV = "Game Development"
    MMO = "Massively Multiplayer"
    VIDEO_360 = "360 Video"
    EPISODIC = "Episodic"

class Category(str, Enum):
    FULL_CONTROLLER_SUPPORT = "Full controller support"
    COMMENTARY_AVAILABLE = "Commentary available"
    STEAM_WORKSHOP = "Steam Workshop"
    SOURCE_SDK = "Includes Source SDK"
    STEAMVR_COLLECTIBLES = "SteamVR Collectibles"
    STEAM_CLOUD = "Steam Cloud"
    VR_SUPPORTED = "VR Supported"
    MODS = "Mods"
    STEAM_ACHIEVEMENTS = "Steam Achievements"
    SINGLE_PLAYER = "Single-player"
    CUSTOM_VOLUME_CONTROLS = "Custom Volume Controls"
    LEVEL_EDITOR = "Includes level editor"
    CO_OP = "Co-op"
    LAN_PVP = "LAN PvP"
    PVP = "PvP"
    IAP = "In-App Purchases"
    CROSS_PLATFORM = "Cross-Platform Multiplayer"
    SURROUND_SOUND = "Surround Sound"
    MODS_HL2 = "Mods (require HL2)"
    FAMILY_SHARING = "Family Sharing"
    CAPTIONS = "Captions available"
    REMOTE_PLAY_TABLET = "Remote Play on Tablet"
    VR_ONLY = "VR Only"
    STATS = "Stats"
    CAMERA_COMFORT = "Camera Comfort"
    CHAT_TTS = "Chat Text-to-speech"
    LEADERBOARDS = "Steam Leaderboards"
    CHAT_STT = "Chat Speech-to-text"
    NARRATED_MENUS = "Narrated Game Menus"
    REMOTE_PLAY_TOGETHER = "Remote Play Together"
    PLAYABLE_NO_TIMED_INPUT = "Playable without Timed Input"
    MOUSE_ONLY = "Mouse Only Option"
    STEREO_SOUND = "Stereo Sound"
    ADJUSTABLE_TEXT_SIZE = "Adjustable Text Size"
    TURN_NOTIFS = "Steam Turn Notifications"
    REMOTE_PLAY_PHONE = "Remote Play on Phone"
    TRADING_CARDS = "Steam Trading Cards"
    TIMELINE = "Steam Timeline"
    HDR = "HDR available"
    ONLINE_PVP = "Online PvP"
    SUBTITLES = "Subtitle Options"
    REMOTE_PLAY_TV = "Remote Play on TV"
    LAN_COOP = "LAN Co-op"
    SPLITSCREEN = "Shared/Split Screen"
    ONLINE_COOP = "Online Co-op"
    VR_SUPPORT = "VR Support"
    TOUCH_ONLY = "Touch Only Option"
    TRACKED_CONTROLLERS = "Tracked Controller Support"
    MMO = "MMO"
    PARTIAL_CONTROLLER = "Partial Controller Support"
    MULTI_PLAYER = "Multi-player"
    SAVE_ANYTIME = "Save Anytime"
    VAC_ENABLED = "Valve Anti-Cheat enabled"
    SPLITSCREEN_COOP = "Shared/Split Screen Co-op"
    SPLITSCREEN_PVP = "Shared/Split Screen PvP"
    ADJUSTABLE_DIFFICULTY = "Adjustable Difficulty"
    COLOR_ALTERNATIVES = "Color Alternatives"
    KEYBOARD_ONLY = "Keyboard Only Option"

class ReviewScoreDescription(str, Enum):
    MOSTLY_POSITIVE = "Mostly Positive"
    FIVE_REVIEWS = "5 user reviews"
    POSITIVE = "Positive"
    MOSTLY_NEGATIVE = "Mostly Negative"
    MIXED = "Mixed"
    SEVEN_REVIEWS = "7 user reviews"
    FOUR_REVIEWS = "4 user reviews"
    OVERWHELMINGLY_NEGATIVE = "Overwhelmingly Negative"
    NEGATIVE = "Negative"
    OVERWHELMINGLY_POSITIVE = "Overwhelmingly Positive"
    NINE_REVIEWS = "9 user reviews"
    EIGHT_REVIEWS = "8 user reviews"
    VERY_POSITIVE = "Very Positive"
    NO_REVIEWS = "No user reviews"
    TWO_REVIEWS = "2 user reviews"
    THREE_REVIEWS = "3 user reviews"
    VERY_NEGATIVE = "Very Negative"
    ONE_REVIEW = "1 user reviews"
    SIX_REVIEWS = "6 user reviews"

# ==========================================================
# 2. SEARCH QUERY MODEL
# ==========================================================
class SearchQuery(BaseModel):
    genres: Optional[List[Genre]] = None
    categories: Optional[List[Category]] = None
    developers: Optional[List[str]] = None
    publishers: Optional[List[str]] = None
    release_date_after: Optional[str] = None
    release_date_before: Optional[str] = None
    review_score_min: Optional[int] = None
    review_score_description: Optional[ReviewScoreDescription] = None
    status: str = "GATHERING"

# ==========================================================
# 3. FASTAPI SETUP
# ==========================================================
app = FastAPI()
client = OpenAI()

conversation_state: dict[str, SearchQuery] = {}

# ==========================================================
# 4. HELPERS
# ==========================================================
def update_state(session_id: str, data: dict) -> SearchQuery:
    current = conversation_state.get(session_id, SearchQuery())
    conversation_state[session_id] = current.model_copy(update=data)
    return conversation_state[session_id]

# ==========================================================
# 5. ENDPOINT
# ==========================================================
@app.post("/chat")
async def chat(session_id: str, user_message: str):
    schema = TypeAdapter(SearchQuery).json_schema()

    messages = [
        {"role": "system", "content": "You are a helpful assistant that collects user preferences about games. \
Ask clarifying questions until you have enough information, then set `status` to READY."},
        {"role": "user", "content": user_message}
    ]

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        tools=[{
            "type": "function",
            "function": {
                "name": "collect_search_query",
                "description": "Gather metadata search parameters from user preferences.",
                "parameters": schema
            }
        }],
        tool_choice="auto"
    )

    msg = response.choices[0].message

    if msg.tool_calls:
        for tool_call in msg.tool_calls:
            if tool_call.function.name == "collect_search_query":
                parsed_args = json.loads(tool_call.function.arguments)
                state = update_state(session_id, parsed_args)

                if state.status == "READY":
                    # Metadata search if structured filters are set
                    if state.genres or state.categories or state.developers or state.publishers:
                        sql, params = metadata_search(
                            genres=state.genres,
                            categories=state.categories,
                            developers=state.developers,
                            publishers=state.publishers,
                            release_date_after=state.release_date_after,
                            release_date_before=state.release_date_before,
                            review_score_min=state.review_score_min,
                            review_score_description=state.review_score_description,
                        )
                        results = execute_query(sql, params, "postgresql://postgres:postgres@localhost:5433/games_scraping")
                    else:
                        results = []
                    return {
                        "message": "Here are some games I found for you!",
                        "search_query": state.model_dump(),
                        "results": results
                    }
                else:
                    return {
                        "message": "Got it, tell me more about your preferences.",
                        "search_query": state.model_dump()
                    }

    return {"message": msg.content}
