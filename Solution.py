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
             "ON DELETE CASCA DE," \
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

    pass

#arkadi HA-GAY
def averageAttendanceInStadium(stadiumID: int) -> float:
    pass

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
    pass

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
    pass

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
    pass

#the one that finished the rest
def getClosePlayers(playerID: int) -> List[int]:
    pass

if __name__ == '__main__':
    #dropTables()
    print("0. Creating all tables")
    createTables()
    print("1. Add Teams")
    addTeam(1)
    addTeam(2)
    print("1. Add Match")
    addMatch(Match(555, 'Domestic', 1, 2))
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