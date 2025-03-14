import pandas as pd
import sqlite3

RPW = 185
MP = 40

conn = sqlite3.connect('./cricket.sqlite')

batting = pd.read_excel('BBL24.xlsx', sheet_name = "Batting")
bowling = pd.read_excel('BBL24.xlsx', sheet_name = "Bowling")
players = pd.read_excel('BBL24.xlsx', sheet_name = "Players")
pp = pd.read_excel('BBL24.xlsx', sheet_name = "PowerPlay")
ps = pd.read_excel('BBL24.xlsx', sheet_name = "PowerSurge")
overs = pd.read_excel('BBL24.xlsx', sheet_name = "Overs")
batting.to_sql('batting', conn, index = False, if_exists='replace')
bowling.to_sql('bowling', conn, index = False, if_exists='replace')
players.to_sql('players', conn, index = False, if_exists='replace')
pp.to_sql('pp', conn, index = False, if_exists='replace')
ps.to_sql('ps', conn, index = False, if_exists='replace')
overs.to_sql('overs', conn, index = False, if_exists='replace')

df = pd.read_sql(
    """
    SELECT PlayerID AS PlayerID, SUM(Runs) AS Runs, SUM(Balls) AS Balls
    FROM batting
    GROUP BY batting.PlayerID
    """,
    conn)

df_pp = pd.read_sql(
    """
    SELECT BatterID AS PlayerID, SUM(Runs) AS PPRuns, SUM(Balls) AS PPBalls
    FROM pp
    GROUP BY BatterID
    """,
    conn)
df_ps = pd.read_sql(
    """
    SELECT BatterID AS PlayerID, SUM(Runs) AS PSRuns, SUM(Balls) AS PSBalls
    FROM ps
    GROUP BY BatterID
    """,
    conn)

df_pp.fillna(0, inplace=True)
df_ps.fillna(0, inplace=True)
df = pd.merge(df, df_pp, on='PlayerID', how='left')
df = pd.merge(df, df_ps, on='PlayerID', how='left')
df.fillna(0, inplace=True)
df['MORuns'] = df['Runs'] - df['PPRuns'] - df['PSRuns']
df['MOBalls'] = df['Balls'] - df['PPBalls'] - df['PSBalls']

lg = pd.read_sql(
    """
    SELECT SUM(Runs) AS Runs, SUM(Balls) AS Balls
    FROM batting
    """,
    conn)
lg_runs = lg['Runs'].iloc[0]
lg_balls = lg['Balls'].iloc[0]

lg = pd.read_sql(
    """
    SELECT SUM(Runs) AS Runs, SUM(Balls) AS Balls
    FROM pp
    """,
    conn)
lg_pp_runs = lg['Runs'].iloc[0]
lg_pp_balls = lg['Balls'].iloc[0]
lg_pp_sr = lg_pp_runs/lg_pp_balls * 100

lg = pd.read_sql(
    """
    SELECT SUM(Runs) AS Runs, SUM(Balls) AS Balls
    FROM ps
    """,
    conn)
lg_ps_runs = lg['Runs'].iloc[0]
lg_ps_balls = lg['Balls'].iloc[0]
lg_ps_sr = lg_ps_runs/lg_ps_balls * 100

lg_mo_runs = lg_runs - lg_pp_runs - lg_ps_runs
lg_mo_balls = lg_balls - lg_pp_balls - lg_ps_balls
lg_mo_sr = lg_mo_runs/lg_mo_balls * 100

df['PPRAA'] = ((df['PPRuns']/df['PPBalls'] * 100) - lg_pp_sr) * df['PPBalls']/100
df['MORAA'] = ((df['MORuns']/df['MOBalls'] * 100) - lg_mo_sr) * df['MOBalls']/100
df['PSRAA'] = ((df['PSRuns']/df['PSBalls'] * 100) - lg_ps_sr) * df['PSBalls']/100
df.fillna(0, inplace=True)
df['BattingRAA'] = df['PPRAA'] + df['MORAA'] + df['PSRAA']

dfc = pd.read_sql(
    """
    SELECT DismissalFielderID AS PlayerID
    FROM batting
    WHERE HowOut = 'Caught'
    OR HowOut = 'Caught wk'
    """,
    conn)
catch_counts = dfc['PlayerID'].value_counts()
dfcc = catch_counts.reset_index()
dfcc.columns = ['PlayerID', 'Catches']
dfcc['PlayerID'] = dfcc['PlayerID'].astype(int)
dfc = pd.read_sql(
    """
    SELECT DismissalFielderID AS PlayerID
    FROM batting
    WHERE HowOut = 'Run Out'
    OR HowOut = 'Stumped'
    """,
    conn)
ro_counts = dfc['PlayerID'].value_counts()
dfro = ro_counts.reset_index()
dfro.columns = ['PlayerID', 'Run Outs']
dfro['PlayerID'] = dfro['PlayerID'].astype(int)
df = pd.merge(df,dfcc, on='PlayerID', how='outer')
df = pd.merge(df,dfro, on='PlayerID', how='outer')
df.fillna(0, inplace=True)

dfpp = pd.read_sql(
    """
    SELECT PlayerID, MatchID1, MatchID2, MatchID3, MatchID4, MatchID5, MatchID6, MatchID7, MatchID8, MatchID9, MatchID10
    FROM players
    """,
    conn)
games_played = dfpp.set_index('PlayerID').notna().sum(axis=1).reset_index(name='GP')
df = pd.merge(df, games_played, on = 'PlayerID', how='outer')

df['CPM'] = df['Catches']/df['GP']
df['RPM'] = df['Run Outs']/df['GP']
lgc = pd.read_sql(
    """
    SELECT COUNT(*) AS Catches
    FROM batting
    WHERE HowOut = 'Caught'
    OR HowOut = 'Caught wk'
    """,
    conn)
lgcpmpp = lgc['Catches'].iloc[0]/38/22
print(lgc['Catches'])
lgr = pd.read_sql(
    """
    SELECT COUNT(*) AS 'Run Outs'
    FROM batting
    WHERE HowOut = 'Run Out'
    OR HowOut = 'Stumped'
    """,
    conn)
lgrpmpp = lgr['Run Outs'].iloc[0]/38/22

df['CRAA'] = (df['CPM'] - lgcpmpp) * df['GP'] * 5
df['RRAA'] = (df['RPM'] - lgrpmpp) * df['GP'] * 5
df['FieldingRAA'] = df['CRAA'] + df['RRAA']


dfbe = pd.read_sql(
    """
    SELECT PlayerID, SUM(Overs) AS Overs, SUM(Runs) AS Runs
    FROM bowling
    GROUP BY PlayerID
    """,
    conn)
lgbe = pd.read_sql(
    """
    SELECT PlayerID, SUM(Overs) AS Overs, SUM(Runs) AS Runs
    FROM bowling
    """,
    conn)
dfbepp = pd.read_sql(
    """
    SELECT BowlerID AS PlayerID, SUM(BallsBowled) AS BallsBowled, SUM(Runs) AS Runs, SUM(Byes) AS Byes, SUM(LegByes) AS LegByes
    FROM overs
    WHERE PP = TRUE
    GROUP BY BowlerID
    """,
    conn)
dfbeps = pd.read_sql(
    """
    SELECT BowlerID AS PlayerID, SUM(BallsBowled) AS BallsBowled, SUM(Runs) AS Runs, SUM(Byes) AS Byes, SUM(LegByes) AS LegByes
    FROM overs
    WHERE PS = TRUE
    GROUP BY BowlerID
    """,
    conn)
lgbepp = pd.read_sql(
    """
    SELECT SUM(BallsBowled) AS BallsBowled, SUM(Runs) AS Runs, SUM(Byes) AS Byes, SUM(LegByes) AS LegByes
    FROM overs
    WHERE PP = TRUE
    """,
    conn)
lgbeps = pd.read_sql(
    """
    SELECT SUM(BallsBowled) AS BallsBowled, SUM(Runs) AS Runs, SUM(Byes) AS Byes, SUM(LegByes) AS LegByes
    FROM overs
    WHERE PS = TRUE
    """,
    conn)
dfbepp['PPOvers'] = dfbepp['BallsBowled']/6
dfbeps['PSOvers'] = dfbeps['BallsBowled']/6
dfbepp['PPRuns'] = dfbepp['Runs'] - dfbepp['Byes'] - dfbepp['LegByes']
dfbeps['PSRuns'] = dfbeps['Runs'] - dfbeps['Byes'] - dfbeps['LegByes']
dfbe = pd.merge(dfbe, dfbepp[['PlayerID', 'PPOvers', 'PPRuns']], on = 'PlayerID', how = 'outer')
dfbe = pd.merge(dfbe, dfbeps[['PlayerID', 'PSOvers','PSRuns']], on = 'PlayerID', how = 'outer')
dfbe.fillna(0, inplace=True)
dfbe['MOOvers'] = dfbe['Overs'] - dfbe['PPOvers'] - dfbe['PSOvers']
dfbe['MORuns'] = dfbe['Runs'] - dfbe['PPRuns'] - dfbe['PSRuns']
dfbe['PPEcon'] = dfbe['PPRuns']/dfbe['PPOvers']
dfbe['MOEcon'] = dfbe['MORuns']/dfbe['MOOvers']
dfbe['PSEcon'] = dfbe['PSRuns']/dfbe['PSOvers']
dfbe.fillna(0, inplace=True)
lg_runs = lgbe['Runs'].iloc[0]
lg_overs = lgbe['Overs'].iloc[0]
lg_pp_runs = lgbepp['Runs'].iloc[0] - lgbepp['Byes'].iloc[0] - lgbepp['LegByes'].iloc[0]
lg_pp_overs = lgbepp['BallsBowled'].iloc[0]/6
lg_ps_runs = lgbeps['Runs'].iloc[0] - lgbeps['Byes'].iloc[0] - lgbeps['LegByes'].iloc[0]
lg_ps_overs = lgbeps['BallsBowled'].iloc[0]/6
lg_mo_runs = lg_runs - lg_pp_runs - lg_ps_runs
lg_mo_overs = lg_overs - lg_pp_overs - lg_ps_overs
lg_pp_econ = lg_pp_runs/lg_pp_overs
lg_mo_econ = lg_mo_runs/lg_mo_overs
lg_ps_econ = lg_ps_runs/lg_ps_overs

dfbe['PPERAA'] = (lg_pp_econ - dfbe['PPEcon'])*dfbe['PPOvers']
dfbe['MOERAA'] = (lg_mo_econ - dfbe['MOEcon'])*dfbe['MOOvers']
dfbe['PSERAA'] = (lg_ps_econ - dfbe['PSEcon'])*dfbe['PSOvers']
dfbe['ERAA'] = dfbe['PPERAA'] + dfbe['MOERAA'] + dfbe['PSERAA']
# Figure out Wickets because DLS does not work

df = pd.merge(df, dfbe, on='PlayerID', how='outer')
df.fillna(0, inplace = True)

dffow = pd.read_sql(
    """
    SELECT DismissalBowlerID AS PlayerID, WicketNo, OverNo, BallNo
    FROM batting
    WHERE DismissalBowlerID IS NOT NULL
    """,
    conn)
resources = pd.read_csv('./DLSChart.csv', header=None)
def proj_change(row):
    WicketNo = int(row['WicketNo'])
    OverNo = int(row['OverNo'])
    BallNo = int(row['BallNo'])
    return resources.loc[OverNo*6+BallNo, WicketNo] - resources.loc[OverNo*6+BallNo, WicketNo - 1]
    
dffow['Proj Diff'] = dffow.apply(proj_change, axis=1)
dfbw = dffow.groupby('PlayerID')['Proj Diff'].sum().reset_index()
df = pd.merge(df, dfbw, on='PlayerID', how='outer')
df['Exp. Diff'] = (df['Proj Diff'].sum()/df['Overs'].sum()) * df['Overs']
df.fillna(0, inplace = True)
df['WRAA'] = df['Proj Diff'] - df['Exp. Diff']
df.fillna(0, inplace = True)

df['BowlingRAA'] = df['ERAA'] + df['WRAA']
df['RAA'] = df['BattingRAA'] + df['FieldingRAA'] + df['BowlingRAA']
adj = df['RAA'].sum()
df['RAA Adj'] = -adj/lg_balls/2 * (df['Balls'] + df['Overs']*6)
df['ARAA'] = df['RAA'] + df['RAA Adj']
df['ReplacementRAA'] = 30 * MP/40 * RPW/lg_balls/2 * (df['Balls'] + df['Overs']*6)
df['RAR'] = df['ARAA'] + df['ReplacementRAA']
df['WAR'] = df['RAR']/RPW

dfp = pd.read_sql(
    """
    SELECT PlayerID, PlayerFirstName AS FirstName, PlayerLastName AS LastName, TeamID AS Team
    FROM players
    """,
    conn)
df = pd.merge(dfp, df, on='PlayerID', how='left')
df.fillna(0, inplace = True)
df.sort_values(by='WAR', ascending=False, inplace=True)
print(df)
print(df[['BattingRAA', 'BowlingRAA', 'FieldingRAA', 'ReplacementRAA']].loc[df['PlayerID'] == 110])
print(df[['WRAA', 'ERAA']].loc[df['PlayerID'] == 110])
df111 = pd.read_sql(
    """
    SELECT SUM(Runs) AS Runs, SUM (Wickets) AS Wickets, SUM(Overs) AS Overs
    FROM bowling
    WHERE PlayerID = 110
    """,
    conn)
df111['Average'] = df111['Runs']/df111['Wickets']
df111['Economy'] = df111['Runs']/df111['Overs']
print(df111)
df112 = pd.read_sql(
    """
    SELECT SUM(Runs) AS Runs, SUM (Balls) AS Balls
    FROM batting
    WHERE PlayerID = 110
    """,
    conn)
print(df112)
print(df[['Catches', 'Run Outs']].loc[df['PlayerID'] == 110])
print(df['WAR'].loc[df['PlayerID'] == 110])
df[['PlayerID', 'FirstName', 'LastName', 'WAR']].to_csv('./WAR40.csv', index = False)
df['WARPG'] = df['WAR']/df['GP']
#df[['PlayerID', 'FirstName', 'LastName', 'WARPG']].to_csv('./WARPG.csv', index = False)
df.to_csv('./WARFull.csv', index = False)
print(df[['FirstName', 'LastName', 'WAR']].loc[df['Team'] == 'BH'])
print(df.groupby('Team')['WAR'].sum().reset_index().sort_values(by='WAR', ascending=False))
