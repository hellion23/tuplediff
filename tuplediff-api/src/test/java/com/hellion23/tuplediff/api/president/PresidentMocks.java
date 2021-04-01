package com.hellion23.tuplediff.api.president;


import com.hellion23.tuplediff.api.config.DBStreamConfig;
import com.hellion23.tuplediff.api.config.SourceConfig;
import com.hellion23.tuplediff.api.config.StreamConfig;
import com.hellion23.tuplediff.api.stream.source.FileStreamSource;
import org.apache.http.client.HttpClient;
import org.h2.jdbcx.JdbcDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import com.github.paweladamski.httpclientmock.HttpClientMock;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.util.stream.Collectors;

public class PresidentMocks {
    public static final HttpClient httpClient = mockHttpClient();
    public static final DataSource dataSource = mockDataSource();
    public static final String baseUrl = "http://localhost";

    /**
     * This is a lightweight mock Apache HttpClient created by Pawel Adamski emulating Mockito type logic to produce
     * HttpResponses the way we want to, rather than making actual http service calls or firing up a heavyweight
     * SpringRunner with MockMVC.
     *
     * https://github.com/PawelAdamski/HttpClientMock
     *
     * This mock HtttpClient returns content which is identical to the File StreamSources, except using the
     * HttpClient. It is intended to support the Streams whose source is HttpStreamSource or CompositeHttpStreamSource.
     *
     * For example when making a call to http://localhost/file/MY_FILE, this mock client will return the contents of
     * the MY_FILE resource with return code 200.
     *
     * @return HttpClient httpClient that responds w/ pre-defined HttpResponses
     */
    static HttpClient mockHttpClient () {
        HttpClientMock mocked = new HttpClientMock();

        PresidentTest.filePaths.entrySet().forEach(filePath -> mocked
            .onGet(url(filePath.getKey()))
            .doReturn(fileContent(filePath.getValue()))
        );
        return mocked;
    }

    /**
     * For the given url http://localhost/file/fileName, create a GET Http request that fetches the contents
     * of that file in the HttpResponse. Return code is 200.
     *
     * @param mock
     * @param fileName
     */
    static void mockGetAndReturn (HttpClientMock mock, String fileName) {
        mock.onGet(url(fileName)).doReturn(fileContent(PresidentTest.path(fileName)));
    }

    /**
     * Creates an H2 in memory DataSource with pre-populated data
     * @return
     */
    static DataSource mockDataSource () {
        String url = "jdbc:h2:mem:test" +
            // This is necessary to prevent the database from being closed when the "last" connection is used.  i.e. every
            // call to getConnection() from the Datasource ends up destroying the database when the Connection is released.
            // Another option to use the INIT=RUNSCRIPT FROM '~/create.sql' syntax to pre-populate the database
            ";DB_CLOSE_DELAY=-1";
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL(url);
        ds.setUser("sa");
        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);

        String createTbl =
                "CREATE TABLE PUBLIC.%s (" +
                        "PRESIDENT_ID INT PRIMARY KEY, " +
                        "FIRST_NAME VARCHAR(255), " +
                        "LAST_NAME VARCHAR(255), " +
                        "PRESIDENT_NO INT, " +
                        " DOB INT, " +
                        " COMMENTS VARCHAR(255) ) "
                        // Source the president tables with the CSV files.
                        + " as SELECT * FROM CSVREAD('%s'); ";

        jdbcTemplate.execute(String.format(createTbl, PresidentTest.presidentLeftTbl,
                PresidentTest.path(PresidentTest.presidentsLeftCsv)));
        jdbcTemplate.execute(String.format(createTbl, PresidentTest.presidentRightTbl,
                PresidentTest.path(PresidentTest.presidentsRightCsv)));
        return ds;
    }

    public static String fileContent(String path) {
        try {
            File file = new File(path);
            FileStreamSource fs = new FileStreamSource(file);
            fs.open();
            return new BufferedReader(fs.getReader()).lines().collect(Collectors.joining("\n"));
        }
        catch (Exception ex) {
            throw new RuntimeException("Error reading file " + path, ex);
        }
    }

    /**
     * Make the MockHttpClient return data based on the filename.
     * @param fileName
     * @return
     */
    public static String url(String fileName ) {
        return baseUrl + "/file/" + fileName;
    }

//    public static SourceConfig

    public static SourceConfig httpSourceConfigOfFile(String file) {
        return httpSourceConfig(url(file));
    }

    /**
     * Create a mock Http GET call of a particular file.
     *
     * @param url
     * @return
     */
    public static SourceConfig httpSourceConfig(String url) {
        return SourceConfig.http().httpClient(httpClient).url(url).build();
    }

    /**
     * Create a mocked DBStreamConfig using
     * @param sql
     * @return
     */

    public static DBStreamConfig dbStreamConfig(String sql) {
        return StreamConfig.sql().datasource(DBStreamConfig.configureActualDataSource(dataSource)).sql(sql).build();
    }

}

