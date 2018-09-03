package org.hasadna.analyze2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.io.*;
import java.time.Clock;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class ReadMakatFileImpl  {


    /**
     * get all departure times for routeId and a specific date
     * (according to this makat file)
     * This is an approximated implementation.
     * Actualy for each date we should read the makat file from that date
     * @param routeId
     * @param date
     * @return  List of departure times, sorted.
     */
    public List<String> findDeparturesByRouteIdAndDate(String routeId, LocalDate date) {
        if ( (allRoutesDepartureTimesForDate == null) || allRoutesDepartureTimesForDate.isEmpty()) {
            return new ArrayList<>();
        }
        if (routeId == null || routeId.equals("")) {
            return null;
        }
        return allRoutesDepartureTimesForDate.get(routeId).departureTimesForDate.get(date);
    }


    public static Clock DEFAULT_CLOCK = Clock.systemDefaultZone();
    public static int YEAR = LocalDate.now(DEFAULT_CLOCK).getYear();

    //@Value("${gtfs.dir.location:/home/evyatar/logs/}")
    public String dirOfGtfsFiles = "/home/evyatar/logs/work/";


    private String makatFileName = "makat2018-08-16.txt";
    private String makatFullPath = dirOfGtfsFiles + makatFileName;

    protected final static Logger logger = LoggerFactory.getLogger("console");


    public Map<String, List<MakatData> > mapByRoute = new HashMap<>();
    public Map<String, MakatData> mapDepartureTimesOfRoute = new HashMap<>();
    //public Map<LocalDateTime, List<MakatData>> mapMakatDataOfRouteByFromDate = new HashMap<>();
    // key - routeId, value - RouteDeparture object
    public Map<String, RouteDepartures> allRoutesDepartureTimesForDate = new HashMap<>();

    public ReadMakatFileImpl() {
    }

    boolean status = false;

    //@PostConstruct
    public void init(int month) {
        if (status) return;
        status = false;
        logger.info("init in PostConstruct started");
        dirOfGtfsFiles = "/home/evyatar/logs/work/";
        if (!dirOfGtfsFiles.endsWith("/")) {
            dirOfGtfsFiles = dirOfGtfsFiles + "/";
        }
        makatFullPath = dirOfGtfsFiles + makatFileName;

        try {
            logger.warn("makatFile read starting - {}", makatFullPath);
            mapByRoute = readMakatFile();
            if (mapByRoute.isEmpty()) {
                throw new IllegalArgumentException("makat file not found");
            }
            logger.info("makat file processing");
            LocalDate today = LocalDate.now();
            //mapDepartureTimesOfRoute = processMapByRoute(mapByRoute, today);
            //int month = today.getMonthValue();
            allRoutesDepartureTimesForDate = processMapByRoute(mapByRoute, month);
            logger.warn("makatFile done");
            status = true;
        }
        catch (Exception ex) {
            logger.error("absorbing unhandled exception during reading makat file ", ex);
        }

        logger.info("init in PostConstruct completed");
    }

    public boolean getStatus() {
        return status;
    }


    /**
     *
     * @return map.
     *      key - routeId,
     *      value - List of all lines in makat file for that route.
     *      each makat file line is represented by a MakatData object
     */
    private Map<String, List<MakatData> > readMakatFile() {
        List<String> lines = readAllLinesOfMakatFileAndDoItFast();
        if (lines.isEmpty()) return new HashMap<>();
        Map<String, List<MakatData> > mapByRoute =
            lines.stream().
                map(line -> parseMakatLine(line)).
                collect(Collectors.groupingBy(MakatData::getRouteId));
        return mapByRoute;
    }

    /**
     *
     * @param mapByRoute
     * @param month
     * @return  map. key - routeId, value - a RouteDepartures object
     */
    private Map<String, RouteDepartures> processMapByRoute(Map<String, List<MakatData>> mapByRoute, int month) {
        Map<String, RouteDepartures> mapOfRouteDepartures = new HashMap<>();
        for (String routeId : mapByRoute.keySet()) {
            RouteDepartures routeDepartures = new RouteDepartures();
            routeDepartures.routeId = routeId;
            routeDepartures.makat = mapByRoute.get(routeId).get(0).makat;
            routeDepartures.departureTimesForDate = populateDepartures(routeId, month, mapByRoute);
            mapOfRouteDepartures.put(routeId, routeDepartures);
        }
        return mapOfRouteDepartures;
    }

    /**
     * collect departure times for this route in this month.
     * collecting from a single makat file.
     * This is an approximation. Actually we should collect for each day from its own makat file.
     * @param routeId
     * @param month
     * @param mapByRoute
     * @return
     */
    private Map<LocalDate,List<String>> populateDepartures(String routeId, int month, Map<String, List<MakatData>> mapByRoute) {
        Map<LocalDate,List<String>> routeDepartures = new HashMap<>();
        List<MakatData> allLines = mapByRoute.get(routeId);
        for (LocalDate day : daysOfMonth(month, YEAR)) {
            // use only text lines that has date range that includes this date
            List<MakatData> linesOfRoute = allLines.stream()
                    .filter(makatData -> makatData.dayOfWeek.equals(day.getDayOfWeek()))
                    .filter(makatData -> !makatData.fromDate.isAfter(day) && !makatData.toDate.isBefore(day))
                    .collect(Collectors.toList());
            // collect all departure times from those text lines
            List<String> departures = linesOfRoute.stream().map(makatData -> makatData.departure).collect(Collectors.toList());
            departures.sort(Comparator.naturalOrder());
            routeDepartures.put(day, departures);
            //logger.trace(" {}, {}", day, departures);
        }

        return routeDepartures;
    }

    public List<LocalDate> daysOfMonth(int month, int year) {
        LocalDate start = LocalDate.of(year, month, 1);
        LocalDate end = start.plusMonths(1);
        List<LocalDate> allDatesInMonth = getDatesBetweenUsingJava8(start, end);
        return allDatesInMonth;
    }

    /**
     * taken from https://www.baeldung.com/java-between-dates
     * @param startDate
     * @param endDate
     * @return
     */
    public static List<LocalDate> getDatesBetweenUsingJava8(LocalDate startDate, LocalDate endDate) {
        long numOfDaysBetween = ChronoUnit.DAYS.between(startDate, endDate);
        return IntStream.iterate(0, i -> i + 1)
                .limit(numOfDaysBetween)
                .mapToObj(i -> startDate.plusDays(i))
                .collect(Collectors.toList());
    }

//    private Map<String, MakatData> processMapByRoute(Map<String, List<MakatData>> mapByRoute, LocalDate startAtDate) {
//        try {
//            // key is routeId+date
//            Map<String, MakatData> mapDepartureTimesOfRoute = new HashMap<>();
//            // find all makats
//            for (String routeId : mapByRoute.keySet()) {
//                logger.trace("processing route {}", routeId);
//                Map<DayOfWeek, List<String>> departureTimesForDate = new HashMap<>();
//                for (int count = 0; count < 7; count++) {
//                    LocalDate date = startAtDate.plusDays(count);
//                    logger.trace("day {}, date {}, weekday {}", count, date, date.getDayOfWeek());
//                    departureTimesForDate.put(date.getDayOfWeek(), new ArrayList<>());
//                    List<MakatData> linesOfRoute = mapByRoute.get(routeId).stream()
//                            .filter(makatData -> makatData.dayOfWeek.equals(date.getDayOfWeek()))
//                            .filter(makatData -> !makatData.fromDate.isAfter(date) && !makatData.toDate.isBefore(date))
//                            .collect(Collectors.toList());
//                    logger.trace("found {} records for route {}", linesOfRoute.size(), routeId);
//                    if (!linesOfRoute.isEmpty()) {
//                        for (MakatData makatData : linesOfRoute) {
//                            departureTimesForDate.get(date.getDayOfWeek()).add(makatData.departure);
//                        }
//                        departureTimesForDate.get(date.getDayOfWeek()).sort(Comparator.naturalOrder());
//                        logger.trace("departureTimesForDate has {} departures", departureTimesForDate.get(date.getDayOfWeek()).size());
//                        logger.trace("departures for date {}: {}", date, departureTimesForDate.get(date.getDayOfWeek()));
//                        MakatData template = linesOfRoute.get(0);
//                        //template.departureTimesForDate = departureTimesForDate;
//                        template.tripId = "";
//                        String key = "x";//generateKey(routeId, date);
//                        mapDepartureTimesOfRoute.put(key, template);
//                        logger.trace("added to mapDepartureTimesOfRoute for key {}: {}", key, template);
//                    }
//                }
//            }
//            return mapDepartureTimesOfRoute;
//        }
//        catch (Exception ex) {
//            logger.error("unhandled", ex);
//            throw ex;
//        }
//    }

    private MakatData parseMakatLine(String line) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
        String[] fields = line.split(",");
        String fromDate = fields[4];
        String toDate = fields[5];
        String dayInWeek = fields[7];
        MakatData makatData = new MakatData(
                fields[1],
                fields[0],
                fields[2],
                fields[3],
                LocalDateTime.parse(fromDate, formatter).toLocalDate(),
                LocalDateTime.parse(toDate, formatter).toLocalDate(),
                fields[6],
                dayFromNumber(dayInWeek),
                fields[8]
        );
        return makatData;
    }

    private DayOfWeek dayFromNumber(String num) {
        Map<String, DayOfWeek> days = new HashMap<>();
        days.put("1", DayOfWeek.SUNDAY);
        days.put("2", DayOfWeek.MONDAY);
        days.put("3", DayOfWeek.TUESDAY);
        days.put("4", DayOfWeek.WEDNESDAY);
        days.put("5", DayOfWeek.THURSDAY);
        days.put("6", DayOfWeek.FRIDAY);
        days.put("7", DayOfWeek.SATURDAY);
        return days.getOrDefault(num, DayOfWeek.SATURDAY);
    }

    public boolean doneOnce = false;
    private List<String> readAllLinesOfMakatFileAndDoItFast() {
        List<String> allLines = new ArrayList<>();
        if (doneOnce) return allLines;
        try{
            File inputF = new File(makatFullPath);
            if (!inputF.exists()) return allLines;
            InputStream inputFS = new FileInputStream(inputF);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
            // skip the header of the csv
            allLines = br.lines().skip(1).collect(Collectors.toList());
            br.close();
        } catch (Exception e) {
            logger.error("while trying to read makat file {}", makatFullPath, e);
            // handling - allLines will remain an empty list

            // such an exception usualy means that the file was not downloaded.
            // It will cause the periodic validation to not find departure times
            // because our current implementation takes them from makat file
            // (an alternative is to get them from GTFS files)
        }
        doneOnce = true;
        return allLines;
    }


    public class RouteDepartures {
        public String routeId;
        public String makat;
        public Map<LocalDate, List<String>> departureTimesForDate;
    }

    private class MakatData {
        public String makat = "";
        public String routeId;
        public String direction;
        public String alternative;
        public LocalDate fromDate;
        public LocalDate toDate;
        public String tripId;
        public DayOfWeek dayOfWeek;
        public String departure;
        //public Map<DayOfWeek, List<String>> departureTimesForDate;

        public MakatData(String makat, String routeId, String direction, String alternative, LocalDate fromDate, LocalDate toDate, String tripId, DayOfWeek dayOfWeek, String departure) {
            this.makat = makat;
            this.routeId = routeId;
            this.direction = direction;
            this.alternative = alternative;
            this.fromDate = fromDate;
            this.toDate = toDate;
            this.tripId = tripId;
            this.dayOfWeek = dayOfWeek;
            this.departure = departure;
        }

        public MakatData() {
            new MakatData("", "", "", "", LocalDate.now(DEFAULT_CLOCK), LocalDate.now(DEFAULT_CLOCK), "", DayOfWeek.SATURDAY, "");
        }


        public String getRouteId() {
            return routeId;
        }

        @Override
        public String toString() {
            String s = "MakatData{" +
                    "makat='" + makat + '\'' +
                    ", routeId='" + routeId + '\'' +
                    ", direction='" + direction + '\'' +
                    ", alternative='" + alternative + '\'' +
                    ", fromDate=" + fromDate +
                    ", toDate=" + toDate +
                    ", tripId='" + tripId + '\'';
//            if (departureTimesForDate == null) {
//                s = s + ", dayOfWeek=" + dayOfWeek +
//                        ", departure='" + departure + '\'' +
//                        '}';
//            }
//            else {
//                s = s + ", departures='" + departureTimesForDate.toString() + '\'' +
//                        '}';
//            }
            return s ;
        }
    }
}
