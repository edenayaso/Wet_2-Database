from typing import List
import Utility.DBConnector as Connector
from Utility.DBConnector import ResultSet
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Match import Match
from Business.Player import Player
from Business.Stadium import Stadium
from psycopg2 import sql


def createTables():
    schema = "CREATE TABLE Team(" \
             "Team_Id INTEGER UNIQUE," \
             "CHECK(Team_Id>0))"
    createEachTable(schema)

    schema = "CREATE TABLE Match(" \
             "Match_Id INTEGER PRIMARY KEY," \
             "Competition VARCHAR(13) NOT NULL," \
             "Home_Team_Id INTEGER NOT NULL REFERENCES Team(Team_Id)" \
             "ON DELETE CASCADE," \
             "Away_Team_Id INTEGER NOT NULL REFERENCES Team(Team_Id)" \
             "ON DELETE CASCADE," \
             "CHECK(Home_Team_Id<>Away_Team_Id))"
    createEachTable(schema)

    schema = "CREATE TABLE Player(" \
             "Player_Id INTEGER PRIMARY KEY," \
             "Team_Id INTEGER REFERENCES Team(Team_Id)" \
             "ON DELETE CASCADE," \
             "Age INTEGER NOT NULL CHECK(Age>0)," \
             "Height INTEGER NOT NULL CHECK(Height>0)," \
             "Preferred_Foot VARCHAR(5) NOT NULL," \
             "CHECK(Player_Id>0))"
    createEachTable(schema)

    schema = "CREATE TABLE Stadium(" \
             "Stadium_Id INTEGER PRIMARY KEY," \
             "Capacity INTEGER NOT NULL CHECK(Capacity>0)," \
             "Belong_to INTEGER REFERENCES Team(Team_Id)" \
             "ON DELETE CASCADE," \
             "UNIQUE(Belong_to), " \
             "CHECK(Stadium_Id>0))"
    createEachTable(schema)

    schema = "CREATE TABLE Scored(" \
             "Player_Id INTEGER REFERENCES Player " \
             "ON DELETE CASCADE," \
             "Match_Id INTEGER REFERENCES Match " \
             "ON DELETE CASCADE," \
             "Goals INTEGER NOT NULL CHECK(Goals>0)," \
             "PRIMARY KEY(Player_Id, Match_Id))"
    createEachTable(schema)

    schema = "CREATE TABLE Took_Place(" \
             "Match_Id INTEGER REFERENCES Match " \
             "ON DELETE CASCA DE," \
             "Stadium_Id INTEGER REFERENCES Stadium " \
             "ON DELETE CASCADE," \
             "Spectators INTEGER NOT NULL CHECK(Spectators>0)," \
             "PRIMARY KEY(Match_Id, Stadium_Id))"
    createEachTable(schema)


def createEachTable(schema):
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(schema)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def clearTables():
    conn = Connector.DBConnector()
    tables_names = sql.SQL("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                           "WHERE TABLE_TYPE={} AND TABLE_SCHEMA={}").format(sql.Literal('BASE TABLE'),
                                                                             sql.Literal('public'))
    result = conn.execute(tables_names)
    for name in result[1].rows:
        clear_table_query = sql.SQL("DELETE FROM {} CASCADE ").format(sql.Identifier(name[0]))
        conn.execute(clear_table_query)
    conn.close()


def dropTables():
    conn = Connector.DBConnector()
    tables_names = sql.SQL("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_TYPE={} AND TABLE_SCHEMA={}").format(sql.Literal('BASE TABLE'), sql.Literal('public'))
    result = conn.execute(tables_names)
    for name in result[1].rows:
        drop_table_query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(name[0]))
        conn.execute(drop_table_query)
    conn.close()


def addTeam(teamID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        add_team_query = sql.SQL("INSERT INTO Team(Team_Id) VALUES({Team_Id})").format(Team_Id=sql.Literal(teamID))
        _ = conn.execute(add_team_query)
    except DatabaseException.ConnectionInvalid:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except Exception:
        return ReturnValue.ERROR
    finally:
        conn.close()
        return ReturnValue.OK

def addMatch(match: Match) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        add_match_query = sql.SQL("INSERT INTO Match(Match_Id, Competition, Home_Team_Id, Away_Team_Id)"
                        " VALUES({Match_Id},{Competition},{Home_Team_Id},{Away_Team_Id})").\
            format(Match_Id=sql.Literal(match.getMatchID()), Competition=sql.Literal(match.getCompetition()),
                   Home_Team_Id=sql.Literal(match.getHomeTeamID()),  Away_Team_Id=sql.Literal(match.getAwayTeamID()))
        _ = conn.execute(add_match_query)
    except DatabaseException.ConnectionInvalid:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except Exception:
        return ReturnValue.ERROR
    finally:
        conn.close()
        return ReturnValue.OK


def getMatchProfile(matchID: int) -> Match:
    conn = None
    res = None
    rows_effected, result = 0, ResultSet()
    conn = Connector.DBConnector()
    query = sql.SQL("SELECT * FROM Match WHERE Match_Id={0}").format(sql.Literal(matchID))
    rows_effected, result = conn.execute(query)
    if rows_effected != 0:
        res = Match(result.rows[0][0], result.rows[0][1], result.rows[0][2], result.rows[0][3])
        conn.close()
        return res
    conn.close()
    return Match.badMatch()


def deleteMatch(match: Match) -> ReturnValue:
    pass

#arkadi HA-GAY
def addPlayer(player: Player) -> ReturnValue:
    """
    Add player to the database
    :param player: player class instance
    :return: Return value assoicated with the result of the action
    """
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        add_player_query = sql.SQL("INSERT INTO PLAYER(Player_Id, Team_Id, Age, Height, Preferred_Foot)"
                                   "VALUES({Player_Id}, {Team_Id}, {Age}, {Height}, {Preferred_Foot});").format(Player_Id=sql.Literal(player.getPlayerID()),
                                    Team_Id=sql.Literal(player.getTeamID()),
                                    Age=sql.Literal(player.getAge()),
                                    Height=sql.Literal(player.getHeight()),
                                    Preferred_Foot=sql.Literal(player.getFoot()))
        _ = conn.execute(add_player_query)
    except DatabaseException.ConnectionInvalid:
        return_value = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        return_value = ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.database_ini_ERROR:
        return_value = ReturnValue.ERROR
    finally:
        conn.close()
        return return_value

#arkadi HA-GAY
def getPlayerProfile(playerID: int) -> Player:
    """
    Returns player profile
    :param playerID: integer
    :return: player class instance
    """
    conn = None
    query_result = None
    error = None
    ret_player = None
    try:
        conn = Connector.DBConnector()
        get_player_query = sql.SQL("SELECT Team_Id, Age, Height, Preferred_Foot FROM PLAYER"
                                   " WHERE Player_Id = {Player_Id};").format(Player_Id=sql.Literal(playerID))

        query_result = conn.execute(get_player_query)
        ret_player = Player(playerID,
                            query_result[1].rows[0][0],
                            query_result[1].rows[0][1],
                            query_result[1].rows[0][2],
                            query_result[1].rows[0][3])
    except DatabaseException:
        error = ReturnValue.ERROR
    finally:
        conn.close()
        if ret_player is None:
            return Player.badPlayer()
        return ret_player

#arkadi HA-GAY
def deletePlayer(player: Player) -> ReturnValue:
    pass

#EDEM
def addStadium(stadium: Stadium) -> ReturnValue:
    pass

#EDEM
def getStadiumProfile(stadiumID: int) -> Stadium:
    pass

#EDEM
def deleteStadium(stadium: Stadium) -> ReturnValue:
    pass


def playerScoredInMatch(match: Match, player: Player, amount: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        player_scored_query = sql.SQL("INSERT INTO Scored(Player_Id, Match_Id, Goals)"
                                      "VALUES({Player_Id},{Match_Id},{Goals})").\
                                    format(Player_Id=sql.Literal(player.getPlayerID()),
                                           Match_Id=sql.Literal(match.getMatchID()),Goals=sql.Literal(amount))
        _ = conn.execute(player_scored_query)
    except DatabaseException.ConnectionInvalid:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return ReturnValue.NOT_EXISTS
    except Exception:
        return ReturnValue.ERROR
    finally:
        conn.close()
        return ReturnValue.OK

#EDEN
def playerDidntScoreInMatch(match: Match, player: Player) -> ReturnValue:
    pass

#EDEM
def matchInStadium(match: Match, stadium: Stadium, attendance: int) -> ReturnValue:
    pass

#arkadi HA-GAY
def matchNotInStadium(match: Match, stadium: Stadium) -> ReturnValue:

    pass

#arkadi HA-GAY
def averageAttendanceInStadium(stadiumID: int) -> float:
    pass

#EDEM
def stadiumTotalGoals(stadiumID: int) -> int:
    pass

#arkadi HA-GAY
def playerIsWinner(playerID: int, matchID: int) -> bool:
    pass

#eden
def getActiveTallTeams() -> List[int]:
    pass

#arkadi HA-GAY
def getActiveTallRichTeams() -> List[int]:
    pass

#eden
def popularTeams() -> List[int]:
    pass

#eden
def getMostAttractiveStadiums() -> List[int]:
    pass

#arkadi HA-GAY
def mostGoalsForTeam(teamID: int) -> List[int]:
    pass

#the one that finished the rest
def getClosePlayers(playerID: int) -> List[int]:
    pass

if __name__ == '__main__':
    print("0. Creating all tables")
    createTables()
    print("1. Add Teams")
    addTeam(1)
    addTeam(2)
    print("1. Add Match")
    addMatch(Match(555,'Domestic',1,2))
    eden = getMatchProfile(555)
    print("3. clear all tables")
    # clearTables()
    player_1 = Player(1, 1, 24, 345, 'right')
    player_2 = Player(2, 1, 24, 555, 'left')
    addPlayer(player_1)
    addPlayer(player_2)
    return_player = getPlayerProfile(1)
    print(return_player)
    # createTables()
    # dropTables()