
def calc_growth(df, df_prev, columns, years_ago=1):
    for column in columns:
        growth_column = "%s_growth" % column
        if years_ago > 1:
            growth_column = "{0}_{1}".format(growth_column, years_ago)
        df[growth_column] = (df[column] / df_prev[column]) ** (1.0 / years_ago) - 1

    return df
