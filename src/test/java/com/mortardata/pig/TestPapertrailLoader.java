package com.mortardata.pig;

import junit.framework.Assert;
import org.apache.commons.lang.StringUtils;
import org.apache.pig.ExecType;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.test.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 *
 */
public class TestPapertrailLoader {

    static PigServer pig;

    private static final String dataDir = "build/test/tmpdata/";
    private static final String jsonInput = "papertrail_loader_json_input";
    private static final String stringInput = " papertrail_loader_json_input";

    @Before
    public void setup() throws IOException {
        pig = new PigServer(ExecType.LOCAL);

        Util.deleteDirectory(new File(dataDir));
        try {
            pig.mkdirs(dataDir);

            Util.createLocalInputFile(dataDir + jsonInput,
                    new String[] {
                            "{243216549875413584\t2013-02-27T00:00:06Z\t2013-02-27T00:00:06Z\t1568461\tgrgdret3452342\t35.354.58.844\tUser\tInfo\trequest{\"AUTH_TYPE\"\t\"cookie\", \"HTTP_REFERER\": \"https://efaahefewfadsfwefewff.com\", \"pid\": 4521, \"SCRIPT_NAME\": \"\", \"REQUEST_METHOD\": \"GET\", \"PATH_INFO\": \"/api/do_stuff\", \"SERVER_PROTOCOL\": \"HTTP/1.1\", \"QUERY_STRING\": \"\", \"CONTENT_LENGTH\": null, \"HTTP_USER_AGENT\": \"fagjgsdlfgowegwekgewgf\", \"user_id\": \"8458vse8fr13518631\", \"SERVER_NAME\": \"blah.foo.com\", \"REMOTE_ADDR\": \"10.245.54.23\", \"hostname\": \"mhg5ruhgjh\", \"SERVER_PORT\": \"443\", \"timestamp\": \"2013-02-27T00:00:05.163509+00:00\", \"controller\": \"api\", \"HTTP_HOST\": \";lkjhgfdfgjhk\", \"thread\": \"Dummy-4\", \"HTTP_X_FORWARDED_FOR\": \"15.25.356.32\", \"action\": \"get_alerts\", \"X_FORWARDED_FOR\": null}}",
                            "{674329857239442343\t2013-02-27T11:22:22Z\t2013-02-27T03:22:22Z\t3254123\tjgerge44332532\t157.54.878.45\tUser\tNotice\trequest{\"AUTH_TYPE\"\tnull, \"HTTP_REFERER\": null, \"pid\": 15784, \"SCRIPT_NAME\": \"\", \"REQUEST_METHOD\": \"GET\", \"PATH_INFO\": \"/login\", \"SERVER_PROTOCOL\": \"HTTP/1.1\", \"QUERY_STRING\": \"\", \"CONTENT_LENGTH\": null, \"HTTP_USER_AGENT\": \"fedghjkh/1.0\", \"user_id\": null, \"SERVER_NAME\": \"10.245.543.25\", \"REMOTE_ADDR\": \"15.544.54.65\", \"hostname\": \"web565435\", \"SERVER_PORT\": \"443\", \"timestamp\": \"2013-02-27T03:22:22.738540+00:00\", \"controller\": \"login\", \"HTTP_HOST\": \"12.264.256.25\", \"thread\": \"MainThread\", \"HTTP_X_FORWARDED_FOR\": null, \"action\": \"index\", \"X_FORWARDED_FOR\": null}}"
                    });

            Util.createLocalInputFile(dataDir + stringInput,
                    new String[] {
                            "{164154641378641312\t2013-02-27T23:59:52Z\t2013-02-28T00:00:00Z\t13545\terfghjgvdf-sdfsdfd\tXX.XX.XXX.XXX\tUser\tInfo\tffheufw/wrdfs.2\t[api] Requested: {\"method\":\"GET\",\"path\":\"/api/fewfqweq/fdqwfqwfr/qwrwqe\",\"query\":\"size=3&cursor=445wr34fsdfsd%tgrergvcwe\"}}",
                            "{954123787415443553\t2013-02-27T23:59:54Z\t2013-02-28T00:00:00Z\t13511\terefre-derew\tXX.XX.XXX.XXX\tUser\tInfo\tdefwe/wew.2\t[api] qdwsfgwrgaetwg. app/sdwqfasdasdasdxs.rb:10 (pid:232)}",
                            "{454154621513584656\t2013-02-28T00:00:00Z\t2013-02-28T00:00:00Z\t44531\tdwewfew-fewwf\tXX.XX.XXX.XXX\tUser\tInfo\tdsfee/cds.3\tStarted GET \"/api/dasdadwfwf\" for XX.XX.XXX.XXX at 2013-02-28 00:00:00 +0000}"
                    });
        }
        catch (IOException e) {};
    }

    @After
    public void cleanup() throws IOException {
        Util.deleteDirectory(new File(dataDir));
        pig.shutdown();
    }

    @Test
    public void jsonTypes() throws IOException {
        pig.registerQuery(
                "data = load '" + dataDir + jsonInput + "' " +
                        "using com.mortardata.pig.PapertrailLoader();"
        );

        Iterator<Tuple> data = pig.openIterator("data");
        String[] expected = {
                "({243216549875413584,2013-02-27T00:00:06Z,2013-02-27T00:00:06Z,1568461,grgdret3452342,35.354.58.844,User,Info,request{\"AUTH_TYPE\"\t\"cookie\", \"HTTP_REFERER\": \"https://efaahefewfadsfwefewff.com\", \"pid\": 4521, \"SCRIPT_NAME\": \"\", \"REQUEST_METHOD\": \"GET\", \"PATH_INFO\": \"/api/do_stuff\", \"SERVER_PROTOCOL\": \"HTTP/1.1\", \"QUERY_STRING\": \"\", \"CONTENT_LENGTH\": null, \"HTTP_USER_AGENT\": \"fagjgsdlfgowegwekgewgf\", \"user_id\": \"8458vse8fr13518631\", \"SERVER_NAME\": \"blah.foo.com\", \"REMOTE_ADDR\": \"10.245.54.23\", \"hostname\": \"mhg5ruhgjh\", \"SERVER_PORT\": \"443\", \"timestamp\": \"2013-02-27T00:00:05.163509+00:00\", \"controller\": \"api\", \"HTTP_HOST\": \";lkjhgfdfgjhk\", \"thread\": \"Dummy-4\", \"HTTP_X_FORWARDED_FOR\": \"15.25.356.32\", \"action\": \"get_alerts\", \"X_FORWARDED_FOR\": null}})",
                "({674329857239442343,2013-02-27T11:22:22Z,2013-02-27T03:22:22Z,3254123,jgerge44332532,157.54.878.45,User,Notice,request{\"AUTH_TYPE\"\tnull, \"HTTP_REFERER\": null, \"pid\": 15784, \"SCRIPT_NAME\": \"\", \"REQUEST_METHOD\": \"GET\", \"PATH_INFO\": \"/login\", \"SERVER_PROTOCOL\": \"HTTP/1.1\", \"QUERY_STRING\": \"\", \"CONTENT_LENGTH\": null, \"HTTP_USER_AGENT\": \"fedghjkh/1.0\", \"user_id\": null, \"SERVER_NAME\": \"10.245.543.25\", \"REMOTE_ADDR\": \"15.544.54.65\", \"hostname\": \"web565435\", \"SERVER_PORT\": \"443\", \"timestamp\": \"2013-02-27T03:22:22.738540+00:00\", \"controller\": \"login\", \"HTTP_HOST\": \"12.264.256.25\", \"thread\": \"MainThread\", \"HTTP_X_FORWARDED_FOR\": null, \"action\": \"index\", \"X_FORWARDED_FOR\": null}})"
        };
        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(data, "\n"));

    }

    @Test
    public void stringTypes() throws IOException {
        pig.registerQuery(
                "data = load '" + dataDir + stringInput + "' " +
                        "using com.mortardata.pig.PapertrailLoader();"
        );

        Iterator<Tuple> data = pig.openIterator("data");
        String[] expected = {
                "({164154641378641312,2013-02-27T23:59:52Z,2013-02-28T00:00:00Z,13545,erfghjgvdf-sdfsdfd,XX.XX.XXX.XXX,User,Info,ffheufw/wrdfs.2\t[api] Requested: {\"method\":\"GET\",\"path\":\"/api/fewfqweq/fdqwfqwfr/qwrwqe\",\"query\":\"size=3&cursor=445wr34fsdfsd%tgrergvcwe\"}})",
                "({954123787415443553,2013-02-27T23:59:54Z,2013-02-28T00:00:00Z,13511,erefre-derew,XX.XX.XXX.XXX,User,Info,defwe/wew.2\t[api] qdwsfgwrgaetwg. app/sdwqfasdasdasdxs.rb:10 (pid:232)})",
                "({454154621513584656,2013-02-28T00:00:00Z,2013-02-28T00:00:00Z,44531,dwewfew-fewwf,XX.XX.XXX.XXX,User,Info,dsfee/cds.3\tStarted GET \"/api/dasdadwfwf\" for XX.XX.XXX.XXX at 2013-02-28 00:00:00 +0000})"
        };
        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(data, "\n"));

    }

    @Test
    public void pushProjection() throws IOException {
        pig.registerQuery(
                "data = load '" + dataDir + jsonInput + "' " +
                        "using com.mortardata.pig.PapertrailLoader();"
        );

        pig.registerQuery(
                "projection = foreach data generate id, message;"
        );

        Iterator<Tuple> projection = pig.openIterator("projection");
        String[] expected = {
                "({243216549875413584,request{\"AUTH_TYPE\"\t\"cookie\", \"HTTP_REFERER\": \"https://efaahefewfadsfwefewff.com\", \"pid\": 4521, \"SCRIPT_NAME\": \"\", \"REQUEST_METHOD\": \"GET\", \"PATH_INFO\": \"/api/do_stuff\", \"SERVER_PROTOCOL\": \"HTTP/1.1\", \"QUERY_STRING\": \"\", \"CONTENT_LENGTH\": null, \"HTTP_USER_AGENT\": \"fagjgsdlfgowegwekgewgf\", \"user_id\": \"8458vse8fr13518631\", \"SERVER_NAME\": \"blah.foo.com\", \"REMOTE_ADDR\": \"10.245.54.23\", \"hostname\": \"mhg5ruhgjh\", \"SERVER_PORT\": \"443\", \"timestamp\": \"2013-02-27T00:00:05.163509+00:00\", \"controller\": \"api\", \"HTTP_HOST\": \";lkjhgfdfgjhk\", \"thread\": \"Dummy-4\", \"HTTP_X_FORWARDED_FOR\": \"15.25.356.32\", \"action\": \"get_alerts\", \"X_FORWARDED_FOR\": null}})",
                "({674329857239442343,request{\"AUTH_TYPE\"\tnull, \"HTTP_REFERER\": null, \"pid\": 15784, \"SCRIPT_NAME\": \"\", \"REQUEST_METHOD\": \"GET\", \"PATH_INFO\": \"/login\", \"SERVER_PROTOCOL\": \"HTTP/1.1\", \"QUERY_STRING\": \"\", \"CONTENT_LENGTH\": null, \"HTTP_USER_AGENT\": \"fedghjkh/1.0\", \"user_id\": null, \"SERVER_NAME\": \"10.245.543.25\", \"REMOTE_ADDR\": \"15.544.54.65\", \"hostname\": \"web565435\", \"SERVER_PORT\": \"443\", \"timestamp\": \"2013-02-27T03:22:22.738540+00:00\", \"controller\": \"login\", \"HTTP_HOST\": \"12.264.256.25\", \"thread\": \"MainThread\", \"HTTP_X_FORWARDED_FOR\": null, \"action\": \"index\", \"X_FORWARDED_FOR\": null}})"
        };
        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(projection, "\n"));
    }
}
