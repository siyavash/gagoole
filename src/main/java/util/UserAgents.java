package util;

public class UserAgents {
    final static String[] userAgents = {"Googlebot/2.1",
            "Mozilla/5.0",
            "Mozilla/4.0",
            "Mozilla/4.0",
            "Mozilla/4.0",
            "Mozilla/4.0",
            "LapozzBot/1.4",
            "Mozilla/4.0",
            "Sqworm/2.9.85-BETA",
            "Gaisbot/3.0",
            "GalaxyBot/1.0",
            "genieBot"
    };

//    final static String[] userAgents = {"Googlebot/2.1 (+http://www.googlebot.com/bot.html)",
//            "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
//            "Mozilla/4.0 (compatible: FDSE robot)",
//            "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2; SV1; .NET CLR 1.1.4322; Girafabot [girafa.com])",
//            "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 4.0; Girafabot; girafabot at girafa dot com; http://www.girafa.com)",
//            "Mozilla/4.0 (compatible; MSIE 5.0; Windows NT; Girafabot; girafabot at girafa dot com; http://www.girafa.com)",
//            "LapozzBot/1.4 (+http://robot.lapozz.com)",
//            "Mozilla/4.0 (compatible; MSIE 5.0; Windows 95) VoilaBot BETA 1.2 (http://www.voila.com/)",
//            "Sqworm/2.9.85-BETA (beta_release; 20011115-775; i686-pc-linux-gnu)",
//            "Gaisbot/3.0+(robot06@gais.cs.ccu.edu.tw;+http://gais.cs.ccu.edu.tw/robot.php)",
//            "GalaxyBot/1.0 (http://www.galaxy.com/galaxybot.html)",
//            "genieBot (http://64.5.245.11/faq/faq.html)"
//    };

    public static String getRandom() {
        int index = (int) (Math.random() * userAgents.length);
        return userAgents[index];
    }
}
