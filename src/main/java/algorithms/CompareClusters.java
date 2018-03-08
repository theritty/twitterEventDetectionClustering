package algorithms;

import cassandraConnector.CassandraDao;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import topologyBuilder.TopologyHelper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

class x {
    long round;
    String text;

    long getRound () { return round;}
}
public class CompareClusters {
    public static void clusterPercentage(String c, String EVENTS_TABLE1, String EVENTS_TABLE2, double perc) throws Exception {
        TopologyHelper topologyHelper = new TopologyHelper();
        Properties properties = topologyHelper.loadProperties( "config.properties" );

        String TWEETS_TABLE = properties.getProperty("clustering.tweets.table");
        String EVENTS_WORDBASED_TABLE = properties.getProperty("clustering.events_wordbased.table");
        String CLUSTER_TABLE = properties.getProperty("clustering.clusters.table");
        String PROCESSEDTWEET_TABLE = properties.getProperty("clustering.processed_tweets.table");
        String PROCESSTIMES_TABLE = properties.getProperty("clustering.processtimes.table");
        String TWEETSANDCLUSTER_TABLE = properties.getProperty("clustering.tweetsandcluster.table");

        CassandraDao cassandraDao1 = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, EVENTS_TABLE1, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE, TWEETSANDCLUSTER_TABLE);
        CassandraDao cassandraDao2 = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, EVENTS_TABLE2, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE, TWEETSANDCLUSTER_TABLE);
        ResultSet resultSetClustering ;
        try {
            resultSetClustering = cassandraDao1.getEvents(c);
            Iterator<Row> iteratorClustering = resultSetClustering.iterator();
            int clusterNum= 0;
            int clusterNum2= 0;
            int clusterInter = 0;

            while (iteratorClustering.hasNext()) {
                Row row = iteratorClustering.next();
                int total=0, intersection=0;
                HashMap<String, Double> cosinevector = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
                UUID clusterid = row.getUUID("clusterid");
                long round = row.getLong("round");
                ResultSet resultSetClustering2 = cassandraDao2.getEvents(c);
                Iterator<Row> iteratorClustering2 = resultSetClustering2.iterator() ;
                clusterNum++;
                clusterNum2=0;

                while(iteratorClustering2.hasNext()) {
                    clusterNum2++;
                    Row row2 = iteratorClustering2.next();
                    HashMap<String, Double> cosinevector2 = (HashMap<String, Double>) row2.getMap("cosinevector", String.class, Double.class);
                    UUID clusterid2 = row2.getUUID("clusterid");
                    long round2 = row2.getLong("round");
                    if(round == round2) {
                        Iterator<Map.Entry<String, Double>> it = cosinevector.entrySet().iterator();
                        ArrayList<String> ks = new ArrayList<>();
                        while(it.hasNext()) {
                            Map.Entry<String, Double> entry = it.next();
                            String key = entry.getKey();
                            total++;

                            Iterator<Map.Entry<String, Double>> it2 = cosinevector2.entrySet().iterator();
                            while (it2.hasNext()) {
                                Map.Entry<String, Double> entry2 = it2.next();
                                String key2 = entry2.getKey();

                                if(key.equals(key2)) {
                                    ks.add(key2);
                                    intersection++;
                                    break;
                                }
                            }
                        }
                        if((double)intersection/(double)total > perc) {
                            System.out.println("Similar found:  " + clusterid + " - " + clusterid2  + ". Percentage: " + (double)intersection/(double)total + ". Words: " + ks );
//                            System.out.println(cosinevector);
//                            System.out.println(cosinevector2);
                            clusterInter++;
                            break;
                        }
                        else {
//                            System.out.println("Similarity  " + clusterid + " - " + clusterid2  + ". Percentage: " + (double)intersection/(double)total );

                        }
                    }
                }
            }

            System.out.println("Final Intersection Percentage: " + (double)clusterInter/(double)clusterNum  + " and " + (double)clusterInter/(double)clusterNum2 + " for similarity percentage threshold of " + perc + " " + clusterInter + " " + clusterNum + " " + clusterNum2);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void xx(double perc) {
        try {
            String EVENTS_TABLE1 = "clusteringevents3";
            String EVENTS_TABLE2 = "hybridevents4";
            System.out.println("CAN_____________________________");
            clusterPercentage("CAN", EVENTS_TABLE1, EVENTS_TABLE2, perc);
            System.out.println("USA_____________________________");
            clusterPercentage("USA", EVENTS_TABLE1, EVENTS_TABLE2, perc);
            System.out.println("**********************************************************************");

            System.out.println("CAN_____________________________");
            clusterPercentage("CAN", EVENTS_TABLE2, EVENTS_TABLE1, perc);
            System.out.println("USA_____________________________");
            clusterPercentage("USA", EVENTS_TABLE2, EVENTS_TABLE1, perc);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws ParseException {

//        String s =
//                                                "31 May 2016, Tuesday, 10:48 AM & USA & 151 & \\{arrive=0.09, back=0.06, better=0.1, big=0.11, cable=0.11, catch=0.07, curry=0.1, example=0.11, fire=0.12, game=0.42, get=0.09, great=0.11, hate=0.11, heating=0.11, klay=1.01, light=0.1, look=0.06, make=0.08, man=0.06, mission=0.08, mvp=0.09, nigga=0.06, past=0.08, play=0.08, player=0.11, prime=0.11, pull=0.11, respect=0.11, right=0.12, save=0.05, say=0.09, shoot=0.12, shooter=0.14, song=0.11, stop=0.09, swear=0.11, take=0.05, thompson=0.93, trouble=0.11, unanimous=0.06, warrior=0.15\\} & [game:0.42, klay:1.01, thompson:0.93] & Thompson knocked down an NBA playoff record 11 3s, Thompson scored 41 points in Game 6 in NBA 2016 Western Conference finals.(Bir playoff maçında en çok üçlük atan oyuncu olmuş bu maçta ve NBA tarihine geçmiş) \\\\  \\hline\n" +
//                                "31 May 2016, Tuesday, 10:54 AM & USA & 750 & \\{act=0.11, adam=0.62, arm=0.09, ass=0.05, bad=0.06, billion=0.06, break=0.06, bueno=0.06, deserve=0.05, dirty=0.17, disgrace=0.06, draymond=0.77, fine=0.06, foul=0.06, game=0.06, get=0.19, good=0.06, green=0.8, hate=0.09, hit=0.06, hurt=0.08, jackass=0.06, kick=0.06, kid=0.05, league=0.1, line=0.06, look=0.06, man=0.08, mess=0.06, night=0.06, play=0.06, player=0.14, point=0.06, pull=0.16, quarter=0.06, quit=0.06, respect=0.06, series=0.06, shit=0.06, sportsman=0.06, steven=0.27, submission=0.06, take=0.08, tap=0.06, try=0.11, understand=0.06, weak=0.07, worst=0.05, would=0.1\\} & [adam:0.62, draymond:0.77, green:0.8, steven:0.27] & Green’s takedown of Adams in Game 7 in NBA 2016 Western Conference finals. \\\\  \\hline\n" +
//                                                        "31 May 2016, Tuesday, 11:30 AM & USA & 169 & \\{adam=0.98, ankle=0.1, back=0.12, bucket=0.06, bust=0.08, center=0.08, cook=0.13, court=0.08, crossover=0.06, curry=0.87, day=0.08, dem=0.06, dirty=0.06, espn=0.08, everytime=0.08, fck=0.06, fight=0.08, get=0.45, gettin=0.09, green=0.08, guard=0.34, help=0.07, island=0.05, keep=0.21, klay=0.1, let=0.09, line=0.05, live=0.08, make=0.11, man=0.16, mouth=0.06, perimeter=0.1, science=0.08, seem=0.06, sense=0.08, series=0.08, shit=0.12, spray=0.17, steven=0.57, stop=0.09, switch=0.16, thompson=0.1, time=0.05, try=0.05\\} & [adam:0.98, curry:0.87, get:0.45, guard:0.34, keep:0.21, steven:0.57] & NBA finals (Steven Adams Stephen Curry ve Klay Thompson a quick little monkeys demis- NBA Western Conference Finals Game 1) \\\\  \\hline\n" +
//                                "31 May 2016, Tuesday, 11:54 AM & USA & 111 & \\{belong=0.09, blues=0.22, bonino=0.11, boy=0.06, cup=0.1, damn=0.14, dion=0.07, effort=0.2, final=0.09, game=0.95, get=0.57, good=0.13, great=0.14, guess=0.2, hell=0.06, huge=0.08, kill=0.07, let=0.18, next=0.14, nigga=0.25, pen=0.71, penguin=0.28, predict=0.08, right=0.2, say=0.08, second=0.08, series=0.07, shark=0.26, stanley=0.09, star=0.17, start=0.09, take=0.58, time=0.07, vega=0.08, waiter=0.17, way=0.09, wednesday=0.2, win=0.6, yah=0.17, yeah=0.07\\} & [blues:0.22, game:0.95, get:0.57, nigga:0.25, pen:0.71, penguin:0.28, shark:0.26, take:0.58, win:0.6] & Penguins win Game 2 against Sharks in overtime in the Stanley Cup Final, NHL 2016   \\\\  \\hline\n" +
//                                "31 May 2016, Tuesday, 12:24 PM & USA & 309 & \\{able=0.05, alot=0.05, ass=0.05, baby=0.04, back=0.35, basketball=0.13, beat=0.22, believe=0.02, better=0.08, brink=0.03, call=0.02, cav=0.81, champ=0.05, championship=0.05, chance=0.06, com=0.03, come=0.37, conference=0.02, crazy=0.02, curry=0.05, damn=0.13, deficit=0.08, durant=0.03, entertaining=0.05, even=0.1, everybody=0.05, fan=0.17, feel=0.03, final=0.12, game=0.17, get=0.22, gon=0.39, good=0.09, gsw=0.02, guess=0.03, hate=0.14, hell=0.02, hope=0.07, jus=0.05, lead=0.03, lebron=0.12, legendary=0.05, let=0.14, lie=0.13, lose=0.14, make=0.02, moment=0.05, much=0.08, nba=0.02, next=0.08, people=0.03, regardless=0.13, ride=0.03, right=0.05, root=0.08, row=0.13, say=0.13, screw=0.03, season=0.08, see=0.06, series=0.3, since=0.08, stomach=0.05, straight=0.04, take=0.08, talk=0.05, team=0.12, tell=0.13, thunder=0.19, time=0.07, title=0.05, understand=0.13, warrior=0.76, way=0.08, western=0.02, whole=0.12, win=0.72, world=0.08, year=0.07\\} & [back:0.35, beat:0.22, cav:0.81, come:0.37, get:0.22, gon:0.39, series:0.3, warrior:0.76, win:0.72] & Golden State Warriors complete comeback to reach 2016 NBA Finals - (http://www.foxnews.com/sports/2016/05/31/golden-state-warriors-complete-comeback-to-reach-nba-finals.html)\\\\  \\hline\n" +
//                                "31 May 2016, Tuesday, 12:24 PM & USA & 175 & \\{ass=0.09, baby=0.12, bandwagon=0.05, basketball=0.1, beat=0.08, better=0.05, call=0.08, cav=0.19, chip=0.09, cleveland=0.15, damn=0.05, definitely=0.09, describe=0.09, drive=0.09, dub=0.06, fan=0.18, final=0.12, game=0.1, get=0.07, golden=1.01, gon=0.06, great=0.09, hate=0.1, hell=0.09, lebron=0.05, lose=0.1, make=0.06, much=0.09, non=0.08, numTweets=0.0, part=0.09, ref=0.09, rig=0.08, see=0.08, series=0.09, sleep=0.1, state=1.01, take=0.1, team=0.06, tonight=0.06, unbelievable=0.08, wait=0.11, warrior=0.08, win=0.18, word=0.18, yeah=0.1\\} & [golden:1.01, state:1.01] & Predictions about 2016 NBA finals \\\\  \\hline\n" +
//                                "31 May 2016, Tuesday, 09:48 PM & USA & 74 & \\{guest=0.98, movie=0.98, please=1.0, see=1.74, trailer=0.98\\} & [guest:0.98, movie:0.98, please:1.0, see:1.74, trailer:0.98] & Trailer of \"The Guest\"\\\\  \\hline\n" +
//                                "01 June 2016, Wednesday, 03:54 PM & USA & 97 & \\{fdb=1.0, music=1.0, official=1.0, ringtone=1.0, video=1.0, ztro=1.0\\} & [fdb:1.0, music:1.0, official:1.0, ringtone:1.0, video:1.0, ztro:1.0] &  Ztro - FDB Ringtone \\\\  \\hline\n" +
//                                "02 June 2016, Thursday, 12:12 PM & USA & 102 & \\{fdb=1.0, music=1.0, numTweets=0.0, official=1.0, ringtone=1.0, video=1.0, ztro=1.0\\} & [fdb:1.0, music:1.0, official:1.0, ringtone:1.0, video:1.0, ztro:1.0] &  Ztro - FDB Ringtone\\\\  \\hline\n" +
//                                "02 June 2016, Thursday, 08:30 PM & USA & 127 & \\{cavalier=0.63, final=1.0, lock=0.37, nba=1.0, take=1.0, warrior=0.37, win=1.0\\} & [cavalier:0.63, final:1.0, lock:0.37, nba:1.0, take:1.0, warrior:0.37, win:1.0] & NBA finals predictions ( June 2, 2016)\\\\  \\hline\n" +
//                                "02 June 2016, Thursday, 08:36 PM & USA & 266 & \\{cavalier=0.62, final=1.0, lock=0.37, nba=1.0, take=1.0, warrior=0.38, win=1.0\\} & [cavalier:0.62, final:1.0, lock:0.37, nba:1.0, take:1.0, warrior:0.38, win:1.0] & predictions about 2016 NBA finals\\\\  \\hline\n" +
//                "03 June 2016, Friday, 08:42 AM & USA & 101 & \\{fdb=0.97, film=0.06, music=1.0, official=1.0, ringtone=0.97, selection=0.06, video=1.0, ztro=0.97\\} & [fdb:0.97, music:1.0, official:1.0, ringtone:0.97, video:1.0, ztro:0.97] & Ztro - FDB Ringtone \\\\  \\hline\n" +
//                                "03 June 2016, Friday, 10:00 AM & USA & 252 & \\{anthem=0.21, banner=0.1, game=0.05, get=0.11, good=0.29, great=0.05, john=0.99, kill=0.08, legend=1.0, love=0.09, man=0.06, national=0.18, okay=0.21, say=0.05, sing=0.18, singing=0.08, sound=0.22, spangled=0.1, star=0.1, voice=0.1\\} & [anthem:0.21, good:0.29, john:0.99, legend:1.0, okay:0.21, sound:0.22] & National anthem - John Legend National Anthem - Game 1 2016 NBA Finals - YouTube https://www.youtube.com/watch?v=YD1ipbCtMKk\\\\  \\hline\n" +
//                                "03 June 2016, Friday, 10:18 AM & USA & 130 & \\{almost=0.09, ankle=0.29, ass=0.05, boy=0.13, bra=0.11, break=0.34, breaker=0.13, cross=0.06, curry=0.31, dirty=0.12, dude=0.08, fall=0.31, floor=0.06, get=0.21, gon=0.07, jab=0.98, make=0.23, man=0.2, map=0.11, nigga=0.12, simple=0.07, slip=0.21, step=0.98, tho=0.07, thompson=0.3, touch=0.1, tristan=0.19, wait=0.13\\} & [ankle:0.29, break:0.34, curry:0.31, fall:0.31, get:0.21, jab:0.98, make:0.23, slip:0.21, step:0.98, thompson:0.3] &NBA finals ( Stephen Curry breaks Tristan Thompson's ankles with a jab step. https://www.youtube.com/watch?v=n7eoTQkoJUg) \\\\  \\hline\n" +
//                                "03 June 2016, Friday, 10:48 AM & USA & 111 & \\{actual=0.06, ass=0.11, baby=0.07, back=0.11, ball=0.11, bang=0.11, better=0.06, block=0.11, blow=0.07, call=0.2, choke=0.08, contact=0.11, damn=0.06, effort=0.07, feel=0.11, foul=0.37, game=0.11, get=0.51, good=0.1, guess=0.06, hell=0.05, kevin=0.9, look=0.11, love=1.0, man=0.13, mess=0.07, moon=0.05, move=0.07, murder=0.11, nice=0.07, play=0.19, pressure=0.07, put=0.06, refuse=0.11, series=0.08, soft=0.23, throw=0.06, trash=0.08, trip=0.11\\} & [foul:0.37, get:0.51, kevin:0.9, love:1.0, soft:0.23] & 2016 NBA finals game 1 kevin love gets an offensive foul but in that position anderson varejao flopped\\\\  \\hline\n" +
//                                "03 June 2016, Friday, 11:54 AM & USA & 242 & \\{announcer=0.05, attempt=0.03, ball=0.54, blow=0.05, calm=0.08, cant=0.05, clearly=0.16, cool=0.08, crotch=0.07, dad=0.12, damn=0.06, delly=0.31, dick=0.41, dude=0.1, face=0.08, foul=0.08, get=0.43, grab=0.07, hard=0.08, hit=0.81, iggy=0.25, iguodala=0.14, iman=0.08, king=0.03, let=0.11, look=0.15, make=0.06, man=0.29, mean=0.03, near=0.04, nigga=0.19, nut=0.6, offense=0.08, person=0.08, piss=0.07, play=0.32, reach=0.03, reaction=0.05, ready=0.05, right=0.07, say=0.13, shumpert=0.11, slide=0.05, steal=0.03, straight=0.09, swing=0.03, swipe=0.03, take=0.08, tap=0.06, tech=0.07, technically=0.04, tha=0.05, try=0.11, well=0.03, whoop=0.03, wrong=0.25\\} & [ball:0.54, delly:0.31, dick:0.41, get:0.43, hit:0.81, iggy:0.25, man:0.29, nut:0.6, play:0.32, wrong:0.25] & 2016 NBA Finals Game 1 - Delly (Matthew Dellavedova) grabbed Iggy’s (Andre Iguodala) junk and the internet had a field day. (https://www.huffingtonpost.com/entry/dellavedova-iguodala-balls\\_us\\_5750f65fe4b0eb20fa0d8311) \\\\  \\hline\n" +
//                                "03 June 2016, Friday, 12:06 PM & USA & 200 & \\{arrogance=0.33, arrogant=0.33, beat=0.15, beautiful=0.06, bench=0.89, better=0.09, big=0.06, call=0.11, carry=0.07, cav=0.59, clean=0.09, cleveland=0.06, curry=0.36, damn=0.11, deep=0.05, even=0.05, fire=0.07, game=0.11, get=0.33, gettin=0.09, golden=0.09, great=0.05, gsw=0.11, kill=0.13, klay=0.06, last=0.09, lead=0.06, light=0.07, livingston=0.09, lose=0.42, miss=0.09, numTweets=0.0, play=0.17, push=0.06, respect=0.33, right=0.07, run=0.09, score=0.09, series=0.06, starter=0.16, state=0.09, take=0.32, terrible=0.09, timeout=0.06, tonight=0.16, torch=0.33, tremendous=0.05, unstoppable=0.06, warrior=0.9, well=0.06, win=0.09, year=0.09\\} & [arrogance:0.33, arrogant:0.33, bench:0.89, cav:0.59, curry:0.36, get:0.33, lose:0.42, respect:0.33, take:0.32, torch:0.33, warrior:0.9] & Cav's looked bad last night. They let a Bench destroy them! With Curry \\& Thompson not shooting well; they couldn't get it done! (Finals game 1) \\\\  \\hline\n" +
//                                "03 June 2016, Friday, 12:06 PM & USA & 204 & \\{apparently=0.08, care=0.1, cav=0.08, cold=0.06, curry=0.07, final=0.12, fire=0.06, freak=0.17, game=0.07, get=0.12, hotspot=0.08, life=0.08, livingston=1.0, man=0.06, mid=0.05, miss=0.24, mvp=0.2, nigga=0.11, numTweets=0.0, play=0.2, real=0.08, respect=0.07, role=0.08, say=0.09, shaun=0.95, son=0.08, tonight=0.08, torch=0.08, voice=0.08, well=0.17, world=0.08\\} & [livingston:1.0, miss:0.24, shaun:0.95] & 2016 NBA finals game 1 de shaun livingston 10da 8 ile muhteşem oynamış ve 20 sayı atmıs bu event onun başarısıyla ilgili\\\\  \\hline\n" +
//                                "03 June 2016, Friday, 12:12 PM & USA & 145 & \\{aye=0.05, ballin=0.06, basically=0.06, beat=0.35, bench=0.9, better=0.08, cav=0.87, cleveland=0.1, compete=0.06, crush=0.06, especially=0.06, even=0.18, expose=0.07, fam=0.06, far=0.06, forget=0.06, game=0.14, get=0.38, hold=0.06, hope=0.1, jock=0.06, kill=0.09, klay=0.14, legit=0.06, light=0.07, look=0.05, lose=0.18, open=0.05, period=0.06, plan=0.06, play=0.09, player=0.1, point=0.08, put=0.08, role=0.07, score=0.2, starter=0.26, step=0.05, stop=0.06, strong=0.06, take=0.07, team=0.08, tonight=0.08, warrior=0.8, whoop=0.08, win=0.08\\} & [beat:0.35, bench:0.9, cav:0.87, get:0.38, starter:0.26, warrior:0.8] & 2016 NBA finals game 1 warriors bench beats cavs starters http://www.nydailynews.com/sports/basketball/nba-finals-game-1-warriors-bench-beats-cavs-starters-article-1.2659345\\\\  \\hline\n" +
//                                "03 June 2016, Friday, 12:30 PM & USA & 109 & \\{bad=0.08, basketball=0.15, bench=0.05, cav=0.65, close=0.13, collective=0.13, curry=0.13, digit=0.08, double=0.08, even=0.12, every=0.07, fine=0.09, focus=0.08, free=0.09, game=0.93, get=0.33, gon=0.12, good=0.13, great=0.09, klay=0.24, kyrie=0.06, lowest=0.26, man=0.06, next=0.4, night=0.08, pizza=0.09, play=0.16, player=0.08, quarter=0.14, remotely=0.11, right=0.07, score=0.27, season=0.27, see=0.07, series=0.16, take=0.25, team=0.17, thompson=0.09, time=0.15, tonight=0.08, warrior=0.58, win=0.95, wrap=0.07\\} & [cav:0.65, game:0.93, get:0.33, klay:0.24, lowest:0.26, next:0.4, score:0.27, season:0.27, take:0.25, warrior:0.58, win:0.95] & 2016 NBA finals game 1 - It was the lowest-scoring combined game for Curry and Thompson all season, yet Golden State won with ease.\\\\  \\hline\n" +
//                                "04 June 2016, Saturday, 01:18 PM & USA & 375 & \\{ali=0.96, bee=0.07, boxer=0.06, butterfly=0.07, damn=0.05, dead=0.06, easy=0.06, float=0.06, goat=0.06, great=0.05, greatest=0.18, legend=0.09, man=0.07, muhamm=0.06, muhammad=0.87, peace=0.18, rest=0.25, rip=0.73, sting=0.07, time=0.08\\} & [ali:0.96, muhammad:0.87, rest:0.25, rip:0.73] & Death of Muhammed Ali\\\\  \\hline\n" +
//                                "04 June 2016, Saturday, 01:24 PM & USA & 1283 & \\{age=0.05, ali=0.84, alus=0.1, athlete=0.03, away=0.01, boxer=0.06, boxing=0.02, break=0.02, butterfly=0.01, champ=0.03, clay=0.05, float=0.01, goat=0.07, great=0.04, greatest=0.27, influential=0.04, king=0.04, legend=0.12, man=0.06, muhamm=0.1, muhammad=0.77, multiple=0.04, pass=0.04, peace=0.23, peacefully=0.04, piece=0.01, report=0.04, rest=0.27, rip=0.69, time=0.11\\} & [ali:0.84, greatest:0.27, muhammad:0.77, peace:0.23, rest:0.27, rip:0.69] & Death of Muhammed Ali\\\\  \\hline\n" +
//                                "04 June 2016, Saturday, 01:24 PM & USA & 174 & \\{champ=0.08, eye=0.11, float=0.88, fly=0.12, greatest=0.08, hand=0.11, hit=0.11, legend=0.11, man=0.08, miss=0.05, muhammad=0.2, numTweets=0.0, peace=0.09, rest=0.12, rip=0.24, see=0.12, sting=0.96\\} & [float:0.88, rip:0.24, sting:0.96] & Death of Muhammed Ali\\\\  \\hline\n" +
//                                "04 June 2016, Saturday, 01:30 PM & USA & 2655 & \\{age=0.01, ali=0.82, alus=0.09, athlete=0.02, away=0.03, boxer=0.08, boxing=0.02, break=0.03, goat=0.06, great=0.02, greatest=0.31, legend=0.11, miss=0.02, muhamm=0.08, muhammad=0.73, news=0.01, numTweets=0.0, pass=0.04, peace=0.15, rest=0.18, rip=0.71, step=0.01, time=0.16\\} & [ali:0.82, greatest:0.31, muhammad:0.73, rip:0.71] & Death of Muhammed Ali\\\\  \\hline\n" +
//                                "04 June 2016, Saturday, 01:30 PM & USA & 304 & \\{champ=0.07, eye=0.06, float=0.9, fly=0.08, goat=0.05, greatest=0.16, hand=0.06, heaven=0.06, hit=0.05, keep=0.05, legend=0.07, legendary=0.06, muhammad=0.15, numTweets=0.0, peace=0.07, rest=0.1, rip=0.29, rumble=0.08, see=0.07, sting=0.96, time=0.1, world=0.05\\} & [float:0.9, rip:0.29, sting:0.96] & Death of Muhammed Ali\\\\  \\hline\n" +
//                                "04 June 2016, Saturday, 01:30 PM & USA & 161 & \\{activist=0.05, ali=0.24, alus=0.18, big=0.48, box=0.08, boxing=0.06, cassius=0.06, champ=0.4, clay=0.08, day=0.06, easy=0.7, fellow=0.51, forget=0.05, goat=0.1, great=0.12, greatest=0.25, immortal=0.05, inspiration=0.1, legend=0.13, man=0.07, many=0.05, miss=0.05, muhamm=0.06, muhammad=0.12, numTweets=0.0, original=0.05, paradise=0.11, peace=0.88, piece=0.43, planet=0.05, rest=1.0, shock=0.05, time=0.12, walk=0.1\\} & [ali:0.24, big:0.48, champ:0.4, easy:0.7, fellow:0.51, greatest:0.25, peace:0.88, piece:0.43, rest:1.0] & Death of Muhammed Ali\\\\  \\hline\n" +
//                                                "04 June 2016, Saturday, 01:36 PM & USA & 151 & \\{ali=0.34, alus=0.14, athlete=0.05, bee=0.08, boxer=0.08, boxing=0.11, butterfly=0.08, career=0.05, champ=0.47, damn=0.06, easy=0.41, finish=0.05, float=0.09, goat=0.09, great=0.06, greatest=0.37, heaven=0.09, history=0.14, leave=0.1, legend=0.29, liston=0.09, louisville=0.06, love=0.06, mohamm=0.09, muhammad=0.28, name=0.06, numTweets=0.0, paradise=0.09, peace=0.85, photo=0.09, piece=0.06, power=0.08, remain=0.08, rest=1.0, ring=0.08, rip=0.09, sport=0.09, stay=0.06, step=0.05, sting=0.09, time=0.15, true=0.1, world=0.1\\} & [ali:0.34, champ:0.47, easy:0.41, greatest:0.37, legend:0.29, muhammad:0.28, peace:0.85, rest:1.0] & Death of Muhammed Ali(June 3, 2016) \\\\  \\hline\n" +
//                                "04 June 2016, Saturday, 01:36 PM & USA & 276 & \\{arm=0.07, champ=0.07, damn=0.05, easy=0.06, eye=0.07, float=0.94, forget=0.05, god=0.07, great=0.06, greatest=0.09, hand=0.07, hit=0.07, legend=0.05, live=0.06, lose=0.06, mohamm=0.05, muhamm=0.05, muhammad=0.21, numTweets=0.0, peace=0.06, rest=0.08, rip=0.28, see=0.07, sting=0.94, time=0.08, today=0.07, well=0.07, youth=0.07\\} & [float:0.94, muhammad:0.21, rip:0.28, sting:0.94] & Death of Muhammed Ali(June 3, 2016)\\\\  \\hline\n" +
//                                                "04 June 2016, Saturday, 01:42 PM & USA & 178 & \\{anymore=0.08, champ=0.05, clay=0.07, eye=0.05, float=0.91, fly=0.11, goat=0.06, greatest=0.1, hand=0.05, hit=0.06, man=0.06, move=0.07, muhamm=0.07, muhammad=0.14, power=0.08, rest=0.1, rip=0.21, rumble=0.15, see=0.05, sky=0.08, sting=0.97, string=0.07, young=0.08\\} & [float:0.91, rip:0.21, sting:0.97] & Death of Muhammed Ali \\\\  \\hline\n" +
//                                "04 June 2016, Saturday, 01:42 PM & USA & 128 & \\{ali=0.28, alus=0.22, boxer=0.05, boxing=0.07, butterfly=0.13, champ=0.36, easy=0.46, family=0.07, fighter=0.07, float=0.1, fly=0.08, forever=0.07, goat=0.09, great=0.15, greatest=0.31, heart=0.07, heavenly=0.08, hero=0.07, learn=0.08, legend=0.2, live=0.06, lose=0.1, muhamm=0.08, muhammad=0.19, numTweets=0.0, peace=0.91, prayer=0.07, rest=1.0, rip=0.07, sweet=0.08, time=0.12, well=0.12\\} & [ali:0.28, alus:0.22, champ:0.36, easy:0.46, greatest:0.31, peace:0.91, rest:1.0] & Death of Muhammed Ali \\\\  \\hline\n" +
//                                "04 June 2016, Saturday, 01:48 PM & USA & 159 & \\{bee=0.95, butterfly=0.97, care=0.07, celebrity=0.07, champ=0.09, easy=0.07, eternity=0.06, eye=0.14, fighter=0.07, float=0.9, fly=0.14, forever=0.07, goat=0.1, great=0.08, greatest=0.09, hand=0.1, heaven=0.08, hit=0.13, legend=0.06, miss=0.07, model=0.07, muhammad=0.15, peace=0.11, power=0.07, rest=0.12, rip=0.18, role=0.07, rumble=0.06, see=0.15, sting=0.97, usually=0.07, way=0.05, world=0.07\\} & [bee:0.95, butterfly:0.97, float:0.9, sting:0.97] & Death of Muhammed Ali\\\\  \\hline\n" +
//                                "04 June 2016, Saturday, 01:54 PM & USA & 123 & \\{butterfly=0.97, champion=0.09, dance=0.08, float=0.93, flow=0.08, fly=0.08, forever=0.16, greatest=0.14, hand=0.1, important=0.08, inspiring=0.08, mohame=0.07, muhammad=0.18, peace=0.07, person=0.08, rest=0.07, rip=0.17, sting=0.99, world=0.06\\} & [butterfly:0.97, float:0.93, sting:0.99] & Death of Muhammed Ali (June 3, 2016 - continues in round 4069550) \\\\  \\hline\n" +
//                                "04 June 2016, Saturday, 02:00 PM & USA & 108 & \\{anyone=0.08, bee=0.95, butterfly=0.98, cadillac=0.08, champ=0.16, easy=0.08, eye=0.09, float=0.95, fly=0.11, forever=0.09, forget=0.09, greatest=0.11, greatness=0.08, hand=0.09, high=0.08, hit=0.09, life=0.08, live=0.08, man=0.06, memorial=0.08, mohamm=0.09, muhamm=0.06, muhammad=0.26, peace=0.11, remember=0.06, rest=0.15, rip=0.24, rumble=0.09, say=0.06, see=0.09, sting=0.98, world=0.08\\} & [bee:0.95, butterfly:0.98, float:0.95, muhammad:0.26, rip:0.24, sting:0.98] & Death of Muhammed Ali(June 3, 2016) \\\\  \\hline\n" +
//                "05 June 2016, Sunday, 12:36 PM & USA & 108 & \\{agree=0.06, app=0.05, awesome=0.18, back=0.5, bring=0.11, brock=0.96, call=0.18, card=0.07, christ=0.06, come=0.15, contract=0.06, deal=0.06, excited=0.06, expire=0.06, fedor=0.13, fight=0.46, get=0.09, gon=0.11, happy=0.17, hell=0.14, holy=0.07, hunt=0.14, imagine=0.17, jesus=0.06, july=0.07, kid=0.17, lesnar=0.86, lesner=0.32, let=0.09, make=0.08, mark=0.14, money=0.07, month=0.17, name=0.06, next=0.17, night=0.06, officially=0.11, opponent=0.07, please=0.07, real=0.05, return=0.07, shit=0.1, take=0.07, tho=0.17, ufc=0.71, vince=0.15, way=0.14, welcome=0.06, wwe=0.06\\} & [back:0.5, brock:0.96, fight:0.46, lesnar:0.86, lesner:0.32, ufc:0.71] & Brock Lesnar is returning to UFC and will be fighting at UFC 200 \\\\  \\hline\n" +
//                "06 June 2016, Monday, 09:00 AM & USA & 109 & \\{almost=0.1, amazing=0.05, anthem=0.99, await=0.1, bad=0.1, beautiful=0.1, boy=0.05, carlo=0.21, carlos=0.1, chile=0.26, chill=0.1, cringe=0.05, different=0.05, disneyland=0.06, drum=0.05, even=0.06, fan=0.06, game=0.1, get=0.06, guitar=0.33, happen=0.05, hate=0.07, hear=0.11, hype=0.1, indian=0.08, instead=0.1, last=0.08, mexican=0.18, mexico=0.07, national=0.95, performance=0.1, play=0.43, put=0.11, response=0.1, right=0.13, rock=0.07, santana=0.37, see=0.06, sick=0.06, sing=0.2, solo=0.1, song=0.05, sound=0.08, stop=0.1, sweet=0.08, trump=0.11, ultimate=0.05, uruguay=0.36, viva=0.05, wife=0.05, william=0.08, wrong=0.19\\} & [anthem:0.99, carlo:0.21, chile:0.26, guitar:0.33, national:0.95, play:0.43, santana:0.37, uruguay:0.36] & Carlos Santana Sings the National Anthem for NBA Finals Game 2  \\\\  \\hline\n" +
//                "06 June 2016, Monday, 10:48 AM & USA & 230 & \\{back=0.14, break=0.05, bron=0.15, call=0.9, career=0.05, damn=0.07, finally=0.24, first=0.08, foul=0.06, game=0.07, get=0.18, happen=0.05, hear=0.05, jame=0.08, keep=0.05, lebron=0.68, little=0.05, lose=0.05, nba=0.16, nigga=0.11, play=0.05, possession=0.07, ref=0.13, rim=0.05, row=0.1, see=0.06, sense=0.05, span=0.05, start=0.11, time=0.19, travel=0.87, travels=0.5, wan=0.08, word=0.05, year=0.06\\} & [call:0.9, finally:0.24, lebron:0.68, travel:0.87, travels:0.5] & LeBron James Travels But Goes Uncalled in 2016 NBA Finals Game 2 \\\\  \\hline\n" +
//                "06 June 2016, Monday, 10:54 AM & USA & 105 & \\{boy=0.2, bron=0.15, cav=0.82, chance=0.09, cleveland=0.2, damn=0.07, game=0.06, get=0.98, gon=0.31, heat=0.2, hope=0.1, joke=0.11, kill=0.05, least=0.11, lebron=0.53, look=0.13, man=0.05, might=0.14, play=0.1, poor=0.09, sense=0.09, series=0.18, seriously=0.07, sweep=0.63, team=0.06, tho=0.06, together=0.21, try=0.1, turnover=0.06, twist=0.06, way=0.08, woulda=0.09\\} & [cav:0.82, get:0.98, gon:0.31, lebron:0.53, sweep:0.63, together:0.21] & Cavaliers sweep Hawks head to ecf Hawks vs cavs nba playoffs \\\\  \\hline\n" +
//                        "06 June 2016, Monday, 02:12 PM & USA & 148 & \\{celebrity=0.09, clearly=0.14, feel=0.12, get=0.66, gon=0.06, hack=0.99, hope=0.07, idol=0.1, kylie=0.82, laugh=0.1, man=0.05, moment=0.05, morning=0.1, must=0.09, night=0.05, play=0.05, probably=0.06, right=0.06, ruin=0.05, shit=0.06, talk=0.1, twitter=0.44, tyler=0.06, wildin=0.06\\} & [get:0.66, hack:0.99, kylie:0.82, twitter:0.44] & Kylie Jenner's Twitter account is HACKED (http://www.dailymail.co.uk/tvshowbiz/article-3626882/Kylie-Jenner-s-Twitter-account-HACKED-lewd-tweets-person-did-Katy-Perry.html)   \\\\  \\hline\n" +
//                                "06 June 2016, Monday, 02:36 PM & USA & 81 & \\{fdb=1.0, music=1.0, official=1.0, ringtone=1.0, video=1.0, ztro=1.0\\} & [fdb:1.0, music:1.0, official:1.0, ringtone:1.0, video:1.0, ztro:1.0] &  Ztro - FDB Ringtone \\\\  \\hline\n" +
//                                        "06 June 2016, Monday, 02:06 PM & CAN & 104 & \\{fdb=100.0, music=100.0, official=100.0, ringtone=100.0, video=100.0, ztro=100.0\\} & [fdb:100.0, music:100.0, official:100.0, ringtone:100.0, video:100.0, ztro:100.0] &  Ztro - FDB Ringtone\\\\  \\hline\n" +
//                        "04 June 2016, Saturday, 01:36 PM & CAN & 121 & \\{boxer=11.0, goat=5.0, great=9.0, greatest=39.0, legend=15.0, man=6.0, muhamm=5.0, muhammad=79.0, paradise=6.0, peace=13.0, rest=17.0, rip=61.0, sport=6.0, time=19.0, world=7.0\\} & [boxer:11.0, goat:5.0, great:9.0, greatest:39.0, legend:15.0, man:6.0, muhamm:5.0, muhammad:79.0, paradise:6.0, peace:13.0, rest:17.0, rip:61.0, sport:6.0, time:19.0, world:7.0] & Death of Muhammed Ali \\\\  \\hline\n" +
//                        "04 June 2016, Saturday, 01:30 PM & CAN & 143 & \\{bee=17.0, boxer=7.0, butterfly=17.0, champ=5.0, float=15.0, greatest=24.0, legend=8.0, man=6.0, muhamm=8.0, muhammad=68.0, rest=7.0, rip=80.0, sting=16.0, time=15.0\\} & [bee:17.0, boxer:7.0, butterfly:17.0, champ:5.0, float:15.0, greatest:24.0, legend:8.0, man:6.0, muhamm:8.0, muhammad:68.0, rest:7.0, rip:80.0, sting:16.0, time:15.0] & Death of Muhammed Ali \\\\  \\hline\n" +
//                        "02 June 2016, Thursday, 10:54 PM & CAN & 71 & \\{cavalier=49.0, final=100.0, lock=46.0, nba=100.0, take=100.0, warrior=49.0, win=100.0\\} & [cavalier:49.0, final:100.0, lock:46.0, nba:100.0, take:100.0, warrior:49.0, win:100.0] &  NBA finals predictions ( June 2, 2016) \\\\ \\hline \\hline\n" +
//                        "31 May 2016, Tuesday, 10:54 AM & CAN & 108 & \\{adam=98.0, ass=9.0, dirty=10.0, draymond=92.0, foul=6.0, get=18.0, green=76.0, hate=7.0, hit=8.0, hurt=11.0, literally=5.0, move=5.0, player=6.0, pull=10.0, steven=48.0, try=9.0, tryna=5.0\\} & [adam:98.0, ass:9.0, dirty:10.0, draymond:92.0, foul:6.0, get:18.0, green:76.0, hate:7.0, hit:8.0, hurt:11.0, literally:5.0, move:5.0, player:6.0, pull:10.0, steven:48.0, try:9.0, tryna:5.0] & NBA finals ( Draymond Green Yanks Steven Adams to the Ground ) \\\\  \\hline\n" ;
//        String s2 = "technical," +
//                "opening," +
//                "latest," +
//                "fit," +
//                "hospitality," +
//                "click," +
//                "opening," +
//                "retail," +
//                "latest," +
//                "opening," +
//                "latest," +
//                "fit," +
//                "retail," +
//                "rest," +
//                "butterfly," +
//                "peace," +
//                "rip," +
//                "greatest," +
//                "muhammad," +
//                "bench," +
//                "double," +
//                "curry," +
//                "win," +
//                "bench," +
//                "step," +
//                "win," +
//                "rest," +
//                "butterfly," +
//                "legend," +
//                "peace," +
//                "rip," +
//                "greatest," +
//                "muhammad," +
//                "rest," +
//                "butterfly," +
//                "peace," +
//                "rip," +
//                "greatest," +
//                "muhammad," +
//                "rest," +
//                "alus," +
//                "butterfly," +
//                "legend," +
//                "peace," +
//                "rip," +
//                "step," +
//                "ali," +
//                "greatest," +
//                "muhammad," +
//                "opening," +
//                "retail," +
//                "latest," +
//                "shaun," +
//                "bench," +
//                "livingston," +
//                "mvp," +
//                "curry," +
//                "win," +
//                "rest," +
//                "alus," +
//                "butterfly," +
//                "legend," +
//                "peace," +
//                "rip," +
//                "greatest," +
//                "muhammad," +
//                "ali," +
//                "rest," +
//                "alus," +
//                "butterfly," +
//                "legend," +
//                "peace," +
//                "rip," +
//                "ali," +
//                "greatest," +
//                "muhammad," +
//                "opening," +
//                "latest," +
//                "rest," +
//                "alus," +
//                "legend," +
//                "peace," +
//                "rip," +
//                "step," +
//                "ali," +
//                "greatest," +
//                "muhammad," +
//                "foul," +
//                "iggy," +
//                "iguodala," +
//                "delly," +
//                "shumpert," +
//                "opening," +
//                "retail," +
//                "latest," +
//                "rest," +
//                "butterfly," +
//                "legend," +
//                "peace," +
//                "rip," +
//                "ali," +
//                "greatest," +
//                "muhammad," +
//                "opening," +
//                "latest," +
//                "foul," +
//                "kevin," +
//                "soft," +
//                "opening," +
//                "retail," +
//                "latest," +
//                "lyft," +
//                "dirty," +
//                "slip," +
//                "jab," +
//                "step," +
//                "ankle," +
//                "curry," +
//                "click," +
//                "opening," +
//                "latest," +
//                "anthem," +
//                "legend," +
//                "national," +
//                "john," +
//                "fit," +
//                "opening," +
//                "latest," +
//                "cavalier," +
//                "nba," +
//                "win," +
//                "hack," +
//                "opening," +
//                "latest," +
//                "cavalier," +
//                "nba," +
//                "win," +
//                "opening," +
//                "latest," +
//                "opening," +
//                "latest," +
//                "opening," +
//                "latest," +
//                "opening," +
//                "latest," +
//                "fit," +
//                "opening," +
//                "latest," +
//                "opening," +
//                "retail," +
//                "latest," +
//                "opening," +
//                "retail," +
//                "latest," +
//                "foul," +
//                "travels," +
//                "ref," +
//                "nba," +
//                "jame," +
//                "vamo," +
//                "opening," +
//                "retail," +
//                "latest," +
//                "opening," +
//                "retail," +
//                "latest," +
//                "ref," +
//                "nba," +
//                "curry," +
//                "win," +
//                "anthem," +
//                "santana," +
//                "national," +
//                "opening," +
//                "retail," +
//                "latest," +
//                "bonino," +
//                "pen," +
//                "dion," +
//                "waiter," +
//                "win," +
//                "dirty," +
//                "adam," +
//                "green," +
//                "steven," +
//                "ankle," +
//                "curry," +
//                "switch," +
//                "mvp," +
//                "curry," +
//                "dirty," +
//                "draymond," +
//                "foul," +
//                "adam," +
//                "green," +
//                "steven," +
//                "opening," +
//                "latest";



        String s= "31 May 2016, Tuesday, 10:54 AM & USA & 465 & \\{act=0.08, adam=0.63, arm=0.11, ass=0.2, bozo=0.06, damn=0.05, dirty=0.82, doin=0.33, draymond=0.69, easily=0.06, fight=0.05, finish=0.05, flop=0.13, foh=0.14, foul=0.1, get=0.17, good=0.04, green=0.73, greene=0.08, hate=0.14, head=0.07, hell=0.05, hellum=0.05, hey=0.05, hit=0.08, hope=0.06, hurt=0.14, idea=0.17, list=0.02, look=0.05, make=0.02, massacre=0.05, nigga=0.03, pick=0.23, piss=0.09, play=0.12, player=0.18, pound=0.05, proud=0.05, pull=0.28, punk=0.02, reason=0.05, respect=0.05, self=0.12, series=0.32, shit=0.16, start=0.02, stay=1.0, steve=0.09, steven=0.17, straight=0.04, take=0.08, top=0.1, trippin=0.02, trump=0.02, try=0.14, ugly=0.37, wait=0.23, would=0.13\\} & [adam:0.63, dirty:0.82, doin:0.33, draymond:0.69, green:0.73, pick:0.23, pull:0.28, series:0.32, stay:1.0, ugly:0.37, wait:0.23] &   NBA finals ( Draymond Green Yanks Steven Adams to the Ground ) \\\\  \\hline\n" +
                "\n" +
                "02 June 2016, Thursday, 08:30 PM & USA & 126 & \\{cavalier=0.63, final=1.0, lock=0.37, nba=1.0, take=1.0, warrior=0.37, win=1.0\\} & [cavalier:0.63, final:1.0, lock:0.37, nba:1.0, take:1.0, warrior:0.37, win:1.0] &  NBA finals predictions ( June 2, 2016)\\\\  \\hline\n" +
                "\n" +
                "03 June 2016, Friday, 10:00 AM & USA & 215 & \\{anthem=0.09, boy=0.06, get=0.13, good=0.08, introduce=0.1, john=1.0, legend=1.01, light=0.25, love=0.08, nigga=0.24, ohio=0.07, sing=0.17, singing=0.08, skin=0.5, take=0.5, tryna=0.5, voice=0.13\\} & [john:1.0, legend:1.01, light:0.25, nigga:0.24, skin:0.5, take:0.5, tryna:0.5] & National anthem - John Legend National Anthem - Game 1 2016 NBA Finals - YouTube\n" +
                "https://www.youtube.com/watch?v=YD1ipbCtMKk\\\\  \\hline\n" +
                "\n" +
                "03 June 2016, Friday, 10:18 AM & USA & 119 & \\{ankle=0.4, break=0.34, curry=0.27, fall=0.38, get=0.17, jab=0.98, make=0.26, man=0.07, nigga=0.12, simple=0.1, slip=0.09, step=0.93, take=0.11, tho=0.08, thompson=0.34, touch=0.11, tristan=0.17\\} & [ankle:0.4, break:0.34, curry:0.27, fall:0.38, jab:0.98, make:0.26, step:0.93, thompson:0.34] & NBA finals ( Stephen Curry breaks Tristan Thompson's ankles with a jab step )\\\\  \\hline\n" +
                "\n" +
                "04 June 2016, Saturday, 01:18 PM & USA & 277 & \\{ali=0.98, dammit=1.0, damn=0.07, dead=0.08, fighter=0.07, goat=0.12, god=0.92, greatest=0.13, legend=0.13, man=0.12, muhamm=0.11, muhammad=0.87, quick=0.07, real=0.07, rip=0.81, see=0.07, time=0.08, true=0.07, world=0.06\\} & [ali:0.98, dammit:1.0, god:0.92, muhammad:0.87, rip:0.81] & Death of Muhammed Ali\\\\ \\hline \\hline\n" +
                "\n" +
                "06 June 2016, Monday, 09:54 AM & USA & 116 & \\{ass=0.07, back=0.38, cav=0.09, deck=0.5, exactly=0.5, get=0.15, help=1.0, hurt=0.18, kevin=0.98, love=0.99, pussy=0.07, right=0.07, rush=0.5, soft=0.25\\} & [back:0.38, deck:0.5, exactly:0.5, help:1.0, kevin:0.98, love:0.99, rush:0.5, soft:0.25] & 2016 NBA finals game 1 kevin love gets an offensive foul but in that position anderson varejao flopped\\\\  \\hline\n" +
                "\n";

        String s2 = "cavalier,nba,win,dirty,draymond,foul,adam,green,steven,flop,slip,jab,step,ankle,curry,anthem,legend,john,legend,rip,ali,greatest,muhammad,kevin,soft";
//        String str = "Jun 6 2016 23:11:52.454 UTC";
//        SimpleDateFormat df = new SimpleDateFormat("MMM dd yyyy HH:mm:ss.SSS zzz");
//        Date date = df.parse(str);
//        long epoch = date.getTime();
//        System.out.println(epoch); // 1055545912454


//        ArrayList<x> xs = new ArrayList<>();
//        String newS = "";
        String[] split = s.replace("\\{",  "").replace("\\}","").replace(" ", "").split("\\\\hline\\n");
        String[] words = s2.split(",");

        for(String a: split) {
            x xo = new x();
            boolean x = false;

            String[] message_ex = a.split("&");

            if(message_ex.length<4)
                continue;
            String[] message = message_ex[3].split(",");
//            xo.round = time;
////            String d = new Date(time*360000).toString();
//
//            SimpleDateFormat formatter = new SimpleDateFormat("dd MMMM yyyy, EEEE, hh:mm a");
//            String d  = formatter.format(new Date(time*360000));
//
//            newS += a.replace(a.split(" &")[0], d) + " \\hline\n";
//            xo.text =a.replace(a.split(" &")[0], d) + " \\hline\n";
//
//            xs.add(xo);
            for(String d: message) {
                boolean t = false;
                for(String b : words) {
                    if(d.split("=")[0].equals(b)) {
                        t = true;
                        break;
                    }
                }
                if(t) {
                    x = true;
                    break;
                }
//                System.out.println("false");
            }
            if(x) System.out.println("true");
            else System.out.println("false");
        }

//        xs.sort(Comparator.comparingLong(x::getRound));
//
//        for(x xx :xs )
//            System.out.println(xx.text);
//
//        System.out.println("Percentage: 0.1************************************************************************************************************************************");
//        xx(0.1);
//        System.out.println("Percentage: 0.2************************************************************************************************************************************");
//        xx(0.2);
//        System.out.println("Percentage: 0.3************************************************************************************************************************************");
//        xx(0.3);
//        System.out.println("Percentage: 0.4************************************************************************************************************************************");
//        xx(0.4);
//        System.out.println("Percentage: 0.5************************************************************************************************************************************");
//        xx(0.5);
//        System.out.println("Percentage: 0.6************************************************************************************************************************************");
//        xx(0.6);
//        System.out.println("Percentage: 0.7************************************************************************************************************************************");
//        xx(0.7);
    }
//    public static void main(String[] args) {
//
//        TopologyHelper topologyHelper = new TopologyHelper();
//        Properties properties = null;
//        try {
//            properties = topologyHelper.loadProperties( "config.properties" );
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        String TWEETS_TABLE = properties.getProperty("clustering.tweets.table");
//        String EVENTS_WORDBASED_TABLE = properties.getProperty("clustering.events_wordbased.table");
//        String EVENTS_TABLE1 = "eventclusterForExperiment5";
//        String CLUSTER_TABLE = properties.getProperty("clustering.clusters.table");
//        String PROCESSEDTWEET_TABLE = properties.getProperty("clustering.processed_tweets.table");
//        String PROCESSTIMES_TABLE = properties.getProperty("clustering.processtimes.table");
//
//        CassandraDao cassandraDao = null;
//        int count = 0;
//        try {
//            cassandraDao = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, EVENTS_TABLE1, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE);
//            ResultSet resultSet = null;
//            try {
//                for(long i=4068480; i<= 4070160; i++) {
//                    resultSet = cassandraDao.getTweetsByRound(i);
//                    Iterator<Row> iterator = resultSet.iterator();
//                    while(iterator.hasNext()) {
//                        iterator.next();
//                        count++;
//                    }
//                }
//                System.out.println("count " + count);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//
//
//    }
// public static void main(String[] args) {
//
//        TopologyHelper topologyHelper = new TopologyHelper();
//        Properties properties = null;
//        try {
//            properties = topologyHelper.loadProperties( "config.properties" );
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        String TWEETS_TABLE = properties.getProperty("clustering.tweets.table");
//        String EVENTS_WORDBASED_TABLE = properties.getProperty("clustering.events_wordbased.table");
//        String EVENTS_TABLE1 = "clusteringevents3";
//        String CLUSTER_TABLE = properties.getProperty("clustering.clusters.table");
//        String PROCESSEDTWEET_TABLE = properties.getProperty("clustering.processed_tweets.table");
//        String PROCESSTIMES_TABLE = properties.getProperty("clustering.processtimes.table");
//        String TWEETSANDCLUSTER_TABLE = properties.getProperty("clustering.tweetsandcluster.table");
//
//
//     System.out.println("\\begin{longtable}{|p{1cm}|p{1cm}|p{1cm}|p{5cm}|p{4cm}|p{4cm}|} \\hline\n" +
//             //             "round & country & number of tweets & cosine vector & common words & comment \\\\ \\hline");
//
//        CassandraDao cassandraDao;
//        try {
//            cassandraDao = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, EVENTS_TABLE1, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE, TWEETSANDCLUSTER_TABLE);
//            try {
//                ResultSet resultSet = cassandraDao.getEvents("USA");
//                    Iterator<Row> iterator = resultSet.iterator();
//                    while(iterator.hasNext()) {
//                        Row row = iterator.next();
//                        HashMap<String, Double> cosinevector2 = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
//                        UUID clusterid2 = row.getUUID("clusterid");
//                        long round2 = row.getLong("round");
//                        Iterator<Map.Entry<String, Double>> it2 = cosinevector2.entrySet().iterator();
//                        ArrayList<String> mostlyUsed = new ArrayList<>();
//                        while (it2.hasNext()) {
//                            Map.Entry<String, Double> entry2 = it2.next();
//                            String key2 = entry2.getKey();
//                            long factor = (long) Math.pow(10, 2);
//                            entry2.setValue((double) Math.round(entry2.getValue() * factor) / 100);
//                            if(entry2.getValue()>0.2) mostlyUsed.add(key2+":"+entry2.getValue());
//                        }
//                        System.out.println( round2 + " & "  + row.getString("country") + " & " + row.getInt("numtweet") + " & " + cosinevector2.toString().replace("{","\\{").replace("}","\\}") + " & " + mostlyUsed + " & \\\\ \\hline" );
//                    }
//
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            try {
//                ResultSet resultSet = cassandraDao.getEvents("CAN");
//                Iterator<Row> iterator = resultSet.iterator();
//                while(iterator.hasNext()) {
//                    Row row = iterator.next();
//                    HashMap<String, Double> cosinevector2 = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
//                    UUID clusterid2 = row.getUUID("clusterid");
//                    long round2 = row.getLong("round");
//                    Iterator<Map.Entry<String, Double>> it2 = cosinevector2.entrySet().iterator();
//                    ArrayList<String> mostlyUsed = new ArrayList<>();
//                    while (it2.hasNext()) {
//                        Map.Entry<String, Double> entry2 = it2.next();
//                        String key2 = entry2.getKey();
//                        long factor = (long) Math.pow(10, 2);
//                        entry2.setValue((double) Math.round(entry2.getValue() * factor));
//                        if(entry2.getValue()>0.2) {
//                            mostlyUsed.add(key2+":"+entry2.getValue());
//                        }
//                    }
//                    System.out.println( round2 + " & "  + row.getString("country") + " & " + row.getInt("numtweet") + " & " + cosinevector2.toString().replace("{","\\{").replace("}","\\}") + " & " + mostlyUsed + " & \\\\ \\hline" );
//                }
//
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//     System.out.println("\\end{longtable}\n");
//
//
//    }


}
