package assignment2;


interface SQLConst {

    // Table names
    String PLOT_SUMMARIES_TABLE    = "plot_summaries";
    String MOVIES_TABLE            = "movies";

    // Queries
    String SQL_DROP_PLOT_SUM_TBL   = "DROP TABLE IF EXISTS " + PLOT_SUMMARIES_TABLE;
    String SQL_CREATE_PLOT_SUM_TBL = "CREATE TABLE " + PLOT_SUMMARIES_TABLE + " (id TEXT NOT NULL, word TEXT NOT NULL, count INTEGER NOT NULL, PRIMARY KEY (id, word))";
    String SQL_DROP_MOVIE_TBL      = "DROP TABLE IF EXISTS " + MOVIES_TABLE;
    String SQL_CREATE_MOVIE_TBL    = "CREATE TABLE " + MOVIES_TABLE + " (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, actors TEXT)";

    String SQL_INSERT_PLOT_WORD    = "INSERT INTO " + PLOT_SUMMARIES_TABLE + " (id, word, count) VALUES (\"%s\", \"%s\", %s)";
    String SQL_INSERT_MOVIE        = "INSERT INTO " + MOVIES_TABLE + " (id, name, actors) VALUES (\"%s\", \"%s\", \"%s\")";

}
