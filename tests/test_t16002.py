from mapping.t16002 import T16002

def test_ps_court_administrative_violation():
    ps = T16002()
    df = ps._court_administrative_violation_df("")
    ps._ps_court_administrative_violation(df)
    print(ps.variables)