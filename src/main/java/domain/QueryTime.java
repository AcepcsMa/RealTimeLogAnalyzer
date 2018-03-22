package domain;

/**
 * Class of query time(date & hour).
 */
public class QueryTime {

    private String date;
    private String hour;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    @Override
    public String toString() {
        return String.format("%s_%s", date, hour);
    }
}
