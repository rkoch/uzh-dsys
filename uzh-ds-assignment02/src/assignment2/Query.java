/*
 * Distributed Systems 2013
 * Assignment 2: Query
 * Remo Koch, 12-728-291
 */
package assignment2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class Query {

    private Connection mConnection;
    private String     mDBPath;


    public int run(String[] pArgs) {
        // validate cli args
        if ((pArgs.length < 2) || (pArgs.length > 2)) {
            printUsage(System.err);
            return 1;
        }

        // Init paths
        File f = new File(pArgs[0]);
        mDBPath = f.getAbsolutePath();

        // Parse query file
        try {
            BufferedReader reader = new BufferedReader(new FileReader(pArgs[1]));
            String line;
            while ((line = reader.readLine()) != null) {
                execQuery(line.trim());
                System.out.println();
            }
            reader.close();
        } catch (Exception pEx) {
            System.err.println("Error reading or executing query");
            pEx.printStackTrace();
        }

        // Close connection
        try {
            closeConnection();
        } catch (Exception pEx) {
            System.err.println("Error closing database connection");
            pEx.printStackTrace();
        }

        return 0;
    }


    private void printUsage(PrintStream pStream) {
        pStream.printf("usage: %s [Local DB Location] [Query File]%n", getClass().getName());
    }


    private void execQuery(String pQuery)
            throws Exception {
        Connection conn = getConnection();

        Statement s = conn.createStatement();

        String query = String.format(SQLConst.SQL_FETCH_QUERY, pQuery);

        ResultSet res = s.executeQuery(query);
        while (res.next()) {
            String movie = res.getString("name");
            String actors = res.getString("actors");
            System.out.printf("%s\t%s%n", movie, actors);
        }
        res.close();

        // Close statement
        s.close();
    }


    private Connection getConnection()
            throws Exception {

        // Load the jdbc driver since we are on the old JDBC API version
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException pEx) {
            throw new Exception("Could not find suitable JDBC driver", pEx);
        }

        if (mDBPath == null) {
            throw new Exception("DB Path is not set");
        }

        // pool the connection
        if ((mConnection == null) || mConnection.isClosed()) {
            String path = String.format(SQLConst.JDBC_SQLITE_FORMAT, mDBPath);
            mConnection = DriverManager.getConnection(path);
        }

        return mConnection;
    }

    private void closeConnection()
            throws SQLException {
        if ((mConnection != null) && !mConnection.isClosed()) {
            mConnection.close();
            mConnection = null;
        }
    }


    // Main

    public static void main(String... pArgs) {
        Query app = new Query();
        int ret = app.run(pArgs);

        System.exit(ret);
    }

}
