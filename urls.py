#! /usr/bin/python
import urllib

NBA_BASE_URL = "http://stats.nba.com/stats/"

def validate_url(url):
  """
  Validate URL and return True except HTTPError or URLError.
  """
  try:
    urllib.urlopen(url)
  except urllib.error.HTTPError as e:
    print("Error code:", e.code)
    print(url)
  except urllib.error.URLError as e:
    print("We failed to reach a server. Reason: ", e.reason)
    print(url)
  else:
    return True


def make_urls_for_game_id(game_id):
  """
  Create urls for all information pertaining to the given game_id
  """
  urls = []
  boxscore_resources = ['boxscoresummaryv2','boxscoreadvancedv2','boxscoremiscv2','boxscorescoringv2','boxscoreusagev2','boxscorefourfactorsv2','boxscoreplayertrackv2']
  boxscore_params = "?EndPeriod=0&EndRange=0&RangeType=0&StartPeriod=0&StartRange=0&GameID="
  for br in boxscore_resources:
    urls.append(NBA_BASE_URL + br + boxscore_params + game_id) 
  playbyplay_params = "?EndPeriod=10&EndRange=55800&RangeType=2&StartPeriod=1&StartRange=0&GameID="
  shotchart_params = "?SeasonType=Regular+Season&LeagueID=00&TeamID=0&PlayerID=0&Outcome=&Location=&Month=0&SeasonSegment=&DateFrom=&DateTo=&OpponentTeamID=0&VsConference=&VsDivision=&Position=&RookieYear=&GameSegment=&Period=0&LastNGames=0&ContextFilter=&ContextMeasure=FG_PCT&display-mode=performance&zone-mode=zone&GameID="
  urls.append(NBA_BASE_URL + 'playbyplayv2' + playbyplay_params + game_id)
  urls.append(NBA_BASE_URL + 'shotchartdetail' + shotchart_params + game_id)
  return urls


def make_scoreboard_url(date):
  """
  Create a scoreboard url for the given date
  """
  scoreboard_params = "scoreboardV2?DayOffset=0&LeagueID=00&gameDate="
  YYYY = str(date.year)
  MM = str(date.month)
  DD = str(date.day)
  url = NBA_BASE_URL + scoreboard_params + "%2F".join((MM,DD,YYYY))
  return url


def fetch_scoreboard_urls(dates):
  """
  Fetch scoreboard URLs by gameDate given a list of datetime objects.
  """
  urls = []
  scoreboard_params = "scoreboardV2?DayOffset=0&LeagueID=00&gameDate="
  for date in dates:
    YYYY = str(date.year)
    MM = str(date.month)
    DD = str(date.day)
    scoreboard_url = NBA_BASE_URL + scoreboard_params + "%2F".join((MM,DD,YYYY)) 
    urls.append(scoreboard_url)
  return(urls)

