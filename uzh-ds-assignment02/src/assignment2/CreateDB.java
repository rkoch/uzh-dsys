/*
 * Distributed Systems 2013
 * Assignment 2: CreateDB
 * Remo Koch, 12-728-291
 */
package assignment2;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class CreateDB {

    private Connection mConnection;
    private String     mDBPath;


    public int run(String[] pArgs) {
        // validate cli args
        if ((pArgs.length < 3) || (pArgs.length > 3)) {
            printUsage(System.err);
            return 1;
        }

        // Init paths
        File f = new File(pArgs[2]);
        mDBPath = f.getAbsolutePath();

        // Create DB
        long start = System.currentTimeMillis();
        try {
            createAndInitDatabase();
        } catch (Exception pEx) {
            System.err.println("Fatal error: Could not create database");
            pEx.printStackTrace();
            return 2;
        }
        long end = System.currentTimeMillis();
//        System.out.printf("Create and init database: %dms%n", end - start);

        // Read and load inverted index (task 3)
        start = System.currentTimeMillis();
        try {
            loadInvertedIndex(new Path(pArgs[0]));
        } catch (Exception pEx) {
            System.err.println("Fatal error: Could not load inverted index or failed to insert to database");
            pEx.printStackTrace();
            return 3;
        }
        end = System.currentTimeMillis();
//        System.out.printf("Load inverted index: %dms%n", end - start);

        // Read and load actors (task 4)
        start = System.currentTimeMillis();
        try {
            loadActors(new Path(pArgs[1]));
        } catch (Exception pEx) {
            System.err.println("Fatal error: Could not load actors or failed to insert to database");
            pEx.printStackTrace();
            return 4;
        }
        end = System.currentTimeMillis();
//        System.out.printf("Load actors: %dms%n", end - start);

        try {
            closeConnection();
        } catch (Exception pEx) {
            System.err.println("Error closing database connection");
            pEx.printStackTrace();
        }

        return 0;
    }


    private void printUsage(PrintStream pStream) {
        pStream.printf("usage: %s [Task 3 HDFS Location] [Task 4 HDFS Location] [Local DB Location]%n", getClass().getName());
    }


    private void createAndInitDatabase()
            throws Exception {
        Connection conn = getConnection();

        Statement s = conn.createStatement();

        // Drop old tables
        s.executeUpdate(SQLConst.SQL_DROP_PLOT_SUM_TBL);
        s.executeUpdate(SQLConst.SQL_DROP_MOVIE_TBL);

        // Create new plot summaries table
        s.executeUpdate(SQLConst.SQL_CREATE_PLOT_SUM_TBL);
        s.executeUpdate(SQLConst.SQL_CREATE_MOVIE_TBL);

        // Close statement
        s.close();
    }

    private void loadInvertedIndex(Path pHdfsPath)
            throws Exception {
        Connection conn = getConnection();
        conn.setAutoCommit(false);

        List<Path> files = new ArrayList<Path>();

        Configuration hadoopConf = new Configuration();
        hadoopConf.addResource(new Path("/home/ds2013/hadoop/hadoop-1.2.1/conf/core-site.xml"));
        hadoopConf.addResource(new Path("/home/ds2013/hadoop/hadoop-1.2.1/conf/hdfs-site.xml"));

        FileSystem fs = FileSystem.get(new URI(hadoopConf.get("fs.default.name")), hadoopConf);

        FileStatus[] fstatus = fs.listStatus(pHdfsPath);

        for (FileStatus entry : fstatus) {
            if (!entry.isDir()) {
                Path p = entry.getPath();
                if (p.getName().startsWith("part-")) {
                    files.add(p);
                }
            }
        }

        Statement s = conn.createStatement();

        for (Path p : files) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));

            int counter = 0;
            String line = null;
            while ((line = br.readLine()) != null) {
                // Parse entry
                String[] split = line.split(" ");
                if (split.length != 3) {
                    throw new Exception("Line [" + line + "] does not have exactly 3 tokens!");
                }

                try {
                    s.executeUpdate(String.format(SQLConst.SQL_INSERT_PLOT_WORD, split[1], split[0], split[2]));
                } catch (Exception pEx) {
                    throw new Exception("Error in line [" + line + "]", pEx);
                }

                if (counter >= 10000) { // only commit each 10'000 inserts
                    counter = 0;
                    conn.commit();
                }
            }

            // Commit anyways after finishing a file
            conn.commit();

            br.close();
        }

        s.close();
    }

    private void loadActors(Path pHdfsPath)
            throws Exception {
        Connection conn = getConnection();
        conn.setAutoCommit(false);

        List<Path> files = new ArrayList<Path>();

        Configuration hadoopConf = new Configuration();
        hadoopConf.addResource(new Path("/home/ds2013/hadoop/hadoop-1.2.1/conf/core-site.xml"));
        hadoopConf.addResource(new Path("/home/ds2013/hadoop/hadoop-1.2.1/conf/hdfs-site.xml"));

        FileSystem fs = FileSystem.get(new URI(hadoopConf.get("fs.default.name")), hadoopConf);

        FileStatus[] fstatus = fs.listStatus(pHdfsPath);

        for (FileStatus entry : fstatus) {
            if (!entry.isDir()) {
                Path p = entry.getPath();
                if (p.getName().startsWith("part-")) {
                    files.add(p);
                }
            }
        }

        Statement s = conn.createStatement();

        for (Path p : files) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));

            int counter = 0;
            String line = null;
            while ((line = br.readLine()) != null) {
                // Escape double quotes for DB inserts
                line = line.replaceAll("\"", "\"\"");

                // Parse entry
                String[] split = line.split("\t", 3);
                String id = split[0];
                String name = split[1];
                String actors = "";
                if (split.length >= 3) {
                    actors = split[2];
                }

                try {
                    s.executeUpdate(String.format(SQLConst.SQL_INSERT_MOVIE, id, name, actors));
                } catch (Exception pEx) {
                    throw new Exception("Error in line [" + line + "]", pEx);
                }

                if (counter >= 10000) { // only commit each 10'000 inserts
                    counter = 0;
                    conn.commit();
                }
            }

            // Commit anyways after each file
            conn.commit();

            br.close();
        }

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
        CreateDB app = new CreateDB();
        int ret = app.run(pArgs);

        System.exit(ret);
    }

}
