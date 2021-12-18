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
             "Competition VARCHAR(13) NOT NULL CHECK (Competition IN('International', 'Domestic'))," \
             "Home_Team_Id INTEGER NOT NULL REFERENCES Team(Team_Id)" \
             "ON DELETE CASCADE," \
             "Away_Team_Id INTEGER NOT NULL REFERENCES Team(Team_Id)" \
             "ON DELETE CASCADE," \
             "CHECK(Home_Team_Id<>Away_Team_Id))"
    createEachTable(schema)

    schema = "CREATE TABLE Player(" \
             "Player_Id INTEGER PRIMARY KEY," \
             "Team_Id INTEGER NOT NULL REFERENCES Team(Team_Id)" \
             "ON DELETE CASCADE," \
             "Age INTEGER NOT NULL CHECK(Age>0)," \
             "Height INTEGER NOT NULL CHECK(Height>0)," \
             "Preferred_Foot VARCHAR(5) NOT NULL CHECK (Preferred_Foot IN('Left', 'Right'))," \
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
             "ON DELETE CASCADE," \
             "Stadium_Id INTEGER REFERENCES Stadium " \
             "ON DELETE CASCADE," \
             "Spectators INTEGER NOT NULL CHECK(Spectators>0)," \
             "PRIMARY KEY(Match_Id, Stadium_Id))"
    createEachTable(schema)

    # schema = "CREATE OR REPLACE VIEW Goals_In_Stadium AS " \
    #          "SELECT S.Player_Id, T.Stadium_Id, S.Goals " \
    #          "FROM Scored S, Took_Place T " \
    #          "WHERE S.Match_Id=T.Match_Id"
    # createEachTable(schema)
    schema = "CREATE OR REPLACE VIEW Goals_In_Stadium AS " \
             "SELECT Player_Id, Stadium_Id, Goals " \
             "FROM Took_Place LEFT OUTER JOIN Scored " \
             "ON Took_Place.Match_Id=Scored.Match_Id"
    createEachTable(schema)

    schema = "CREATE OR REPLACE VIEW Active_Teams AS " \
             "SELECT Home_Team_Id FROM Match " \
             "UNION " \
             "SELECT Away_Team_Id FROM Match"
    createEachTable(schema)

    schema = "CREATE OR REPLACE VIEW Home_Teams_Stadiums AS " \
             "SELECT M.Home_Team_Id, T.Spectators ,T.Stadium_Id " \
             "FROM Match M, Took_Place T " \
             "WHERE M.Match_Id=T.Match_Id"
    createEachTable(schema)

    schema = "CREATE OR REPLACE VIEW Player_Join_Scored AS " \
             "SELECT Team_Id, Player_Id, Match_Id, goals " \
             "FROM Player NATURAL JOIN Scored;"
    createEachTable(schema)

    schema = "CREATE OR REPLACE VIEW Player_Overall_Scored AS " \
             "SELECT Player_Id, Team_Id, SUM(goals) " \
             "FROM Player_Join_Scored " \
             "GROUP BY Player_Id, Team_Id;"
    createEachTable(schema)

    schema = "CREATE OR REPLACE VIEW Team_Scored_On_Match AS " \
             "SELECT Team_Id, Match_Id, SUM(goals) " \
             "FROM Player_Join_Scored " \
             "GROUP BY Team_Id, Match_Id;"
    createEachTable(schema)

    schema = "CREATE OR REPLACE VIEW Goals_In_Match AS " \
             "SELECT Match_Id, SUM(goals) " \
             "FROM Scored GROUP BY Match_Id;"
    createEachTable(schema)

    schema = "CREATE OR REPLACE VIEW Goals_In_Match_Join_Scored AS " \
             "SELECT * " \
             "FROM Scored NATURAL JOIN Goals_In_Match"
    createEachTable(schema)

    schema = "CREATE OR REPLACE VIEW Active_Tall_Teams AS " \
             "SELECT P.Team_Id " \
             "FROM PLAYER P, ACTIVE_TEAMS T " \
             "WHERE P.TEAM_Id=T.Home_Team_Id AND P.Height>190 " \
             "GROUP BY P.Team_Id " \
             "HAVING COUNT(P.Player_Id)>=2"
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
    conn.execute("DROP VIEW Goals_In_Stadium")
    conn.execute("DROP VIEW Active_Teams")
    tables_names = sql.SQL("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_TYPE={} AND TABLE_SCHEMA={}").format(sql.Literal('BASE TABLE'), sql.Literal('public'))
    result = conn.execute(tables_names)
    for name in result[1].rows:
        drop_table_query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(name[0]))
        conn.execute(drop_table_query)
    conn.close()


def addTeam(teamID: int) -> ReturnValue:
    """
    Add Team to the database
    :param teamID: teamID to be added
    :return: Return value assoicated with the result of the action
    """
    ret_value, conn = None, None
    try:
        conn = Connector.DBConnector()
        add_team_query = sql.SQL("INSERT INTO Team(Team_Id) "
                                 "VALUES({Team_Id})").format(Team_Id=sql.Literal(teamID))
        _ = conn.execute(add_team_query)
        ret_value = ReturnValue.OK
    except DatabaseException.ConnectionInvalid:
        ret_value = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        ret_value = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        ret_value = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        ret_value = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        ret_value = ReturnValue.BAD_PARAMS
    except DatabaseException.database_ini_ERROR:
        ret_value = ReturnValue.ERROR
    except DatabaseException.UNKNOWN_ERROR:
        ret_value = ReturnValue.ERROR
    finally:
        conn.close()
        return ret_value


def addMatch(match: Match) -> ReturnValue:
    """
    Add Match to the database
    :param match: match class instance
    :return: Return value assoicated with the result of the action
    """
    ret_value, conn = None, None
    try:
        conn = Connector.DBConnector()
        add_match_query = sql.SQL("INSERT INTO Match(Match_Id, Competition, Home_Team_Id, Away_Team_Id) "
                                  "VALUES({Match_Id},{Competition},{Home_Team_Id},{Away_Team_Id})").\
            format(Match_Id=sql.Literal(match.getMatchID()), Competition=sql.Literal(match.getCompetition()),
                   Home_Team_Id=sql.Literal(match.getHomeTeamID()),  Away_Team_Id=sql.Literal(match.getAwayTeamID()))
        _ = conn.execute(add_match_query)
        ret_value = ReturnValue.OK
    except DatabaseException.ConnectionInvalid:
        ret_value = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        ret_value = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        ret_value = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        ret_value = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        ret_value = ReturnValue.BAD_PARAMS
    except DatabaseException.database_ini_ERROR:
        ret_value = ReturnValue.ERROR
    except DatabaseException.UNKNOWN_ERROR:
        ret_value = ReturnValue.ERROR
    finally:
        conn.close()
        return ret_value


def getMatchProfile(matchID: int) -> Match:
    """
    Returns match profile  of matchID
    :param matchID: integer
    :return: match class instance
    """
    conn, res = None, None
    # rows_effected, result = 0, ResultSet()
    ret_match = Match()
    try:
        conn = Connector.DBConnector()
        get_match_profile_query = sql.SQL("SELECT * FROM Match WHERE Match_Id={0}").format(sql.Literal(matchID))
        rows_effected, result = conn.execute(get_match_profile_query)
        if rows_effected == 1:
            ret_match.setMatchID(result.rows[0][0])
            ret_match.setCompetition(result.rows[0][1])
            ret_match.setHomeTeamID(result.rows[0][2])
            ret_match.setAwayTeamID(result.rows[0][3])
        else:
            ret_match = Match.badMatch()
    except DatabaseException:
        ret_match = Match.badMatch()
    finally:
        conn.close()
        return ret_match



def deleteMatch(match: Match) -> ReturnValue:
    """
    Delete a match from the database
    :param match: match class instance
    :return: Return value assoicated with the result of the action
    """
    ret_value, conn = None, None
    try:
        conn = Connector.DBConnector()
        delete_match_query = sql.SQL("DELETE FROM Match WHERE Match_Id={0}").format(sql.Literal(match.getMatchID()))
        rows_effected, _ = conn.execute(delete_match_query)
        if rows_effected == 0:
            ret_value = ReturnValue.NOT_EXISTS
        else:
            ret_value = ReturnValue.OK
    except DatabaseException.ConnectionInvalid:
        ret_value = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        ret_value = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        ret_value = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        ret_value = ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        ret_value = ReturnValue.NOT_EXISTS # TODO: check the foreign key error
    except DatabaseException.database_ini_ERROR:
        ret_value = ReturnValue.ERROR
    except DatabaseException.UNKNOWN_ERROR:
        ret_value = ReturnValue.ERROR
    finally:
        conn.close()
        return ret_value

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

def getPlayerProfile(playerID: int) -> Player:
    """
    Returns player profile
    :param playerID: integer
    :return: player class instance
    """
    conn = None
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
        conn.close()
        return Player.badPlayer()
    finally:
        conn.close()
        if ret_player is None:
            return Player.badPlayer()
        return ret_player


# arkadi HA-GAY
def deletePlayer(player: Player) -> ReturnValue:
    conn = None
    query_result = None
    try:
        conn = Connector.DBConnector()
        delete_player_query = sql.SQL("DELETE FROM PLAYER "
                                      "WHERE Player_Id = {Player_Id};") \
            .format(Player_Id=sql.Literal(player.getPlayerID()))
        query_result = conn.execute(delete_player_query)
    except DatabaseException.ConnectionInvalid:
        conn.close()
        return ReturnValue.ERROR
    except DatabaseException.UNKNOWN_ERROR:
        conn.close()
        return ReturnValue.ERROR
    finally:
        conn.close()
        if query_result[0] == 0:
            return ReturnValue.NOT_EXISTS
        return ReturnValue.OK

#EDEM
def addStadium(stadium: Stadium) -> ReturnValue:
    """
    Add Stadium to the database
    :param stadium:  stadium class instance
    :return: Return value assoicated with the result of the action
    """
    ret_value, conn = None, None
    try:
        conn = Connector.DBConnector()
        add_stadium_query = sql.SQL("INSERT INTO Stadium(Stadium_Id, Capacity, Belong_to)"
                                    " VALUES({Stadium_Id},{Capacity},{Belong_to})"). \
            format(Stadium_Id=sql.Literal(stadium.getStadiumID()), Capacity=sql.Literal(stadium.getCapacity()),
                   Belong_to=sql.Literal(stadium.getBelongsTo()))
        _ = conn.execute(add_stadium_query)
        # if rows_effected == 0:
        #     ret_value = ReturnValue.BAD_PARAMS
        # else:
        ret_value = ReturnValue.OK
    except DatabaseException.ConnectionInvalid:
        ret_value = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        ret_value = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        ret_value = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        ret_value = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        ret_value = ReturnValue.BAD_PARAMS
    except DatabaseException.database_ini_ERROR:
        ret_value = ReturnValue.ERROR
    except DatabaseException.UNKNOWN_ERROR:
        ret_value = ReturnValue.ERROR
    finally:
        conn.close()
        return ret_value


#EDEM
def getStadiumProfile(stadiumID: int) -> Stadium:
    """
    Returns stadium profile of stadiumID
    :param stadiumID: integer
    :return: stadium class instance
    """
    conn, res = None, None
    # rows_effected, result = 0, ResultSet()
    ret_stadium = Stadium()
    try:
        conn = Connector.DBConnector()
        get_match_profile_query = sql.SQL("SELECT * FROM Stadium WHERE stadium_Id={0}").format(sql.Literal(stadiumID))
        rows_effected, result = conn.execute(get_match_profile_query)
        if rows_effected == 1:
            ret_stadium.setStadiumID(result.rows[0][0])
            ret_stadium.setCapacity(result.rows[0][1])
            ret_stadium.setBelongsTo(result.rows[0][2])
        else:
            ret_stadium = Stadium.badStadium()
    except DatabaseException:
        ret_stadium = Stadium.badStadium()
    finally:
        conn.close()
        return ret_stadium

#EDEM
def deleteStadium(stadium: Stadium) -> ReturnValue:
    """
    Delete a stadium from the database
    :param stadium: stadium class instance
    :return: Return value assoicated with the result of the action
    """
    ret_value, conn = None, None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Match WHERE Stadium_Id={0}").format(sql.Literal(stadium.getStadiumID()))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            ret_value = ReturnValue.NOT_EXISTS
        else:
            ret_value = ReturnValue.OK
    except DatabaseException.ConnectionInvalid:
        ret_value = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        ret_value = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        ret_value = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        ret_value = ReturnValue.ERROR
    # except DatabaseException.FOREIGN_KEY_VIOLATION:
    #     ret_value = ReturnValue.NOT_EXISTS
    except DatabaseException.database_ini_ERROR:
        ret_value = ReturnValue.ERROR
    except DatabaseException.UNKNOWN_ERROR:
        ret_value = ReturnValue.ERROR
    finally:
        conn.close()
        return ret_value


def playerScoredInMatch(match: Match, player: Player, amount: int) -> ReturnValue:
    """
    Player has scored amount of goals in match
    :param  match: match class instance
            player: player class instance
            amount: integer
    :return: Return value assoicated with the result of the action
    """
    ret_value, conn = None, None
    try:
        conn = Connector.DBConnector()
        player_scored_query = sql.SQL("INSERT INTO Scored(Player_Id, Match_Id, Goals)"
                                      "VALUES({Player_Id},{Match_Id},{Goals})").\
            format(Player_Id=sql.Literal(player.getPlayerID()),
                   Match_Id=sql.Literal(match.getMatchID()),
                   Goals=sql.Literal(amount))
        rows_effected, _ = conn.execute(player_scored_query)
        if rows_effected == 0:
            ret_value = ReturnValue.NOT_EXISTS
        else:
            ret_value = ReturnValue.OK
    except DatabaseException.ConnectionInvalid:
        ret_value = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        ret_value = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        ret_value = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        ret_value = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        ret_value = ReturnValue.NOT_EXISTS
    except DatabaseException.database_ini_ERROR:
        ret_value = ReturnValue.ERROR
    except DatabaseException.UNKNOWN_ERROR:
        ret_value = ReturnValue.ERROR
    finally:
        conn.close()
        return ret_value

#EDEN
def playerDidntScoreInMatch(match: Match, player: Player) -> ReturnValue:
    """
    Player didnt scored in match, if the player did, delete its record
    :param  match: match class instance
            player: player class instance
            amount: integer
    :return: Return value assoicated with the result of the action
    """
    ret_value, conn = None, None
    # rows_effected = 0
    try:
        conn = Connector.DBConnector()
        player_unscored_query = sql.SQL("DELETE FROM Scored WHERE Player_Id={0} AND Match_Id={0}").\
            format(sql.Literal(player.getPlayerID()), sql.Literal(match.getMatchID()))
        rows_effected, _ = conn.execute(player_unscored_query)
        if rows_effected == 1:
            ret_value = ReturnValue.OK
        elif rows_effected == 0:
            ret_value = ReturnValue.NOT_EXISTS
        else:
            ret_value = ReturnValue.ERROR
    except DatabaseException.ConnectionInvalid:
        ret_value = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        ret_value = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION:
        ret_value = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        ret_value = ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        ret_value = ReturnValue.NOT_EXISTS
    except DatabaseException.database_ini_ERROR:
        ret_value = ReturnValue.ERROR
    except DatabaseException.UNKNOWN_ERROR:
        ret_value = ReturnValue.ERROR
    finally:
        conn.close()
        return ret_value

#EDEM
def matchInStadium(match: Match, stadium: Stadium, attendance: int) -> ReturnValue:
    """
    The match is taking place in stadium with attendance spectators
    :param  match: match class instance
            stadium: stadium class instance
            attendance: integer
    :return: Return value assoicated with the result of the action
    """
    ret_value, conn = None, None
    try:
        conn = Connector.DBConnector()
        player_scored_query = sql.SQL("INSERT INTO Took_Place(Match_Id, Stadium_Id, Spectators)"
                                      "VALUES({Match_Id},{Stadium_Id},{Spectators})"). \
            format(Match_Id=sql.Literal(match.getMatchID()),
                   Stadium_Id=sql.Literal(stadium.getStadiumID()),
                   Spectators=sql.Literal(attendance))
        rows_effected, _ = conn.execute(player_scored_query)
        if rows_effected == 0:
            ret_value = ReturnValue.NOT_EXISTS
        else:
            ret_value = ReturnValue.OK
    except DatabaseException.ConnectionInvalid:
        ret_value = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        ret_value = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        ret_value = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        ret_value = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        ret_value = ReturnValue.NOT_EXISTS
    except DatabaseException.database_ini_ERROR:
        ret_value = ReturnValue.ERROR
    except DatabaseException.UNKNOWN_ERROR:
        ret_value = ReturnValue.ERROR
    finally:
        conn.close()
        return ret_value

#arkadi HA-GAY
def matchNotInStadium(match: Match, stadium: Stadium) -> ReturnValue:
    """
    Check if given match is being played in given stadium
    :param match: Match
    :param stadium: Stadium
    :return: ReturnValue
    """
    query_result, conn = None, None
    try:
        conn = Connector.DBConnector()
        match_not_in_stadium_query = sql.SQL("DELETE FROM Took_Place "
                                             "WHERE Match_Id = {Match_Id} AND Stadium_Id = {Stadium_Id};").format(Match_Id=sql.Literal(match.getMatchID()), Stadium_Id=sql.Literal(stadium.getStadiumID()))
        query_result = conn.execute(match_not_in_stadium_query)

    except DatabaseException.ConnectionInvalid:
        conn.close()
        return ReturnValue.ERROR
    finally:
        conn.close()
        if query_result[0] == 0:
            return ReturnValue.NOT_EXISTS
        return ReturnValue.OK


def averageAttendanceInStadium(stadiumID: int) -> float:
    """
    Calculates average specatators for a give stadium
    :param stadiumID: integer
    :return: float
    """
    query_result, conn = None, None
    try:
        conn = Connector.DBConnector()
        average_attendance_in_stadium_query = sql.SQL("SELECT AVG(Spectators) "
                                                      "FROM Took_Place "
                                                      "WHERE Stadium_Id={0}").format(sql.Literal(stadiumID))

        query_result = conn.execute(average_attendance_in_stadium_query)

    except DatabaseException:
        conn.close()
        return -1
    except FloatingPointError:
        conn.close()
        return 0
    finally:
        conn.close()
        if query_result[1].rows[0][0] is None:
            return 0
        return query_result[1].rows[0][0]



#EDEM
def stadiumTotalGoals(stadiumID: int) -> int:
    """
    Returns the total amount of goals scored in stadium with stadiumID
    :param  stadiumID: integer
    :return: ret_sum: integer
    """
    ret_sum, conn = None, None
    # rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        stadium_tot_goals_query = sql.SQL("SELECT COALESCE(SUM(Goals), 0) "
                                          "FROM Goals_In_Stadium "
                                          "WHERE Stadium_Id={0}").format(sql.Literal(stadiumID))
        rows_effected, result = conn.execute(stadium_tot_goals_query)
        if rows_effected == 1:
            ret_sum = result.rows[0][0]
        elif rows_effected == 0:
            ret_sum = 0
        else:
            ret_sum = -1
    except DatabaseException:
        ret_sum = -1
    finally:
        conn.close()
        return ret_sum

#arkadi HA-GAY
def playerIsWinner(playerID: int, matchID: int) -> bool:
    """
    Decides if a player is a winner in a given match
    :param playerID: integer
    :param matchID: integer
    :return: boolean
    """
    conn, query_result = None, None
    try:
        conn = Connector.DBConnector()
        winner_player_query = sql.SQL("SELECT goals, sum "
                                      "FROM Goals_In_Match_Join_Scored "
                                      "WHERE Match_Id={Match_Id} AND Player_Id={Player_Id};").format(Match_Id=sql.Literal(matchID), Player_Id=sql.Literal(playerID))
        query_result = conn.execute(winner_player_query)
    except DatabaseException:
        conn.close()
        return False
    finally:
        conn.close()
        if query_result is None:
            return False
        elif query_result[0] == 0:
            return False
        elif query_result[1].rows[0][1] == 0:
            return False
        elif query_result[1].rows[0][0] / query_result[1].rows[0][1] >= 0.5:
            return True
        else:
            return False


#eden
def getActiveTallTeams() -> List[int]:
    """
    Returns ta list(up to size 5) of active teams'ID that have at least 2 players over the height of 190cm.
    Active team is a team who played at least 1 match at home or away
    :param  None
    :return: active_tall_teams_list: list
    """
    conn = None
    active_tall_teams_list = []
    try:
        conn = Connector.DBConnector()
        result = conn.execute("SELECT P.Team_Id "
                              "FROM Player P, Active_Teams T "
                              "WHERE P.Team_Id=T.Home_Team_Id AND P.Height>190 "
                              "GROUP BY P.Team_Id "
                              "HAVING COUNT(P.Player_Id)>=2 "
                              "ORDER BY P.Team_Id DESC "
                              "LIMIT 5")
        for team in result[1].rows:
            active_tall_teams_list.append(team[0])
    except DatabaseException:
        active_tall_teams_list = []
    finally:
        conn.close()
        return active_tall_teams_list

#arkadi HA-GAY
def getActiveTallRichTeams() -> List[int]:
    """
    Returns the active tallest and richest teams
    :return: list of integers
    """
    query_result, conn = None, None
    active_rich_tall_teams_list = []
    try:
        conn = Connector.DBConnector()
        active_rich_tall_teams_query = ("SELECT Team_Id "
                                        "FROM Active_Tall_Teams INNER JOIN Stadium "
                                        "ON Team_Id=belong_to "
                                        "WHERE Capacity > 55000 "
                                        "ORDER BY Team_Id "
                                        "ASC LIMIT 5;")
        query_result = conn.execute(active_rich_tall_teams_query)
    except DatabaseException:
        conn.close()
        return []
    finally:
        conn.close()
        if query_result[0] == 0:
            return []
        for team_id in query_result[1].rows[0]:
            active_rich_tall_teams_list.append(team_id)
        return active_rich_tall_teams_list

#eden
def popularTeams() -> List[int]:
    """
    Returns a list (up to size 10) of teams' IDs that in every single game they played as 'home team'
    they had more than 40,000 attendance.
    :param  None
    :return: popular_teams_list: list
    """
    conn = None
    popular_teams_list = []
    try:
        conn = Connector.DBConnector()
        result = conn.execute("SELECT Home_Team_Id "
                              "FROM Home_Teams_Stadiums "
                              "GROUP BY Home_Team_Id "
                              "HAVING MIN(Spectators)>40000 "
                              "UNION "
                              "SELECT Away_Team_Id "
                              "FROM Match "
                              "WHERE Away_Team_Id NOT IN(SELECT Home_Team_Id FROM Match) "
                              "ORDER BY Home_Team_Id DESC "
                              "LIMIT 10")
        for team in result[1].rows:
            popular_teams_list.append(team[0])
    except DatabaseException:
        popular_teams_list = []
    finally:
        conn.close()
        return popular_teams_list


#eden
def getMostAttractiveStadiums() -> List[int]:
    """
    Returns a list containing attractive stadiums' IDs.
    The most attractive stadium is the stadium in which the most goals were scored in its matches, and so on.
    The list is ordered by attractiveness in descending, and in case of equality is ordered by ID in ascending
    :param  None
    :return: most_attractive_stadiums_list: list
    """
    conn = None
    most_attractive_stadiums_list = []
    try:
        conn = Connector.DBConnector()
        result = conn.execute("SELECT Stadium_Id, COALESCE(SUM(Goals), 0) "
                              "FROM Goals_In_Stadium "
                              "GROUP BY Stadium_Id "
                              "ORDER BY COALESCE(SUM(Goals), 0) DESC, Stadium_Id ASC")
        for team in result[1].rows:
            most_attractive_stadiums_list.append(team[0])
    except DatabaseException:
        most_attractive_stadiums_list = []
    finally:
        conn.close()
        return most_attractive_stadiums_list

#arkadi HA-GAY
def mostGoalsForTeam(teamID: int) -> List[int]:
    """
    Returns the players that scored most goals for a given team
    :param teamID: integer
    :return:list of integers
    """
    conn, query_result = None, None
    most_goals_for_team = []
    try:
        conn = Connector.DBConnector()
        most_goals_for_team_query = sql.SQL("SELECT Player_Id "
                                            "FROM Player_Overall_Scored "
                                            "WHERE Team_Id={Team_Id} "
                                            "ORDER BY goals, Player_Id DESC "
                                            "LIMIT 10;").format(Team_Id=sql.Literal(teamID))
        query_result = conn.execute(most_goals_for_team_query)
    except DatabaseException:
        return []
    finally:
        if query_result[0] == 0:
            return []
        for player in query_result[1].rows[0]:
            most_goals_for_team.append(player)
        return most_goals_for_team

def getClosePlayers(playerID: int) -> List[int]:
    pass

if __name__ == '__main__':
    #dropTables()
    print("0. Creating all tables")
    createTables()
    print("1. Add Teams")
    addTeam(1)
    addTeam(2)
    addTeam(3)
    addTeam(4)
    print("1. Add Match")
    match_1 = Match(555, 'Domestic', 1, 2)
    match_2 = Match(666, 'Domestic', 3, 4)
    addMatch(match_1)
    addMatch(match_2)
    stadium_1 = Stadium(5, 5000, 2)
    stadium_2 = Stadium(6, 1000, 3)
    addStadium(stadium_1)
    addStadium(stadium_2)
    player_1 = Player(1, 1, 24, 345, 'right')
    player_2 = Player(2, 1, 24, 555, 'left')
    player_3 = Player(3, 2, 24, 555, 'right')
    player_4 = Player(4, 2, 24, 888, 'right')
    addPlayer(player_1)
    addPlayer(player_2)
    addPlayer(player_3)
    addPlayer(player_4)
    return_player = getPlayerProfile(1)
    matchInStadium(match_1, stadium_1, 500)
    matchInStadium(match_2, stadium_1, 600)
    average_1 = averageAttendanceInStadium(stadium_1.getStadiumID())
    average_2 = averageAttendanceInStadium(stadium_2.getStadiumID())
    matchNotInStadium(match_1, stadium_1)
    matchNotInStadium(match_2, stadium_1)
    average_1 = averageAttendanceInStadium(stadium_1.getStadiumID())
    playerScoredInMatch(match_1, player_1, 10)
    playerScoredInMatch(match_1, player_2, 2)
    playerScoredInMatch(match_1, player_3, 3)
    ret_1 = playerIsWinner(1, 555)
    ret_2 = playerIsWinner(2, 555)
    ret_3 = playerIsWinner(3, 555)
    ret_4 = playerIsWinner(1, 777)
    active_tall_teams = getActiveTallTeams()
    rich_active_tall_teams = getActiveTallRichTeams()
    print('Test end\'s here')
    clearTables()
