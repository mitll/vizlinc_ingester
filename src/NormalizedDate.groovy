public class NormalizedDate {

    private String day;
    private String month;
    private String year;
    private static final MONTH_LONG = ["enero", "febrero", "marzo", "abril", "mayo", "junio",
                                       "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
    private static final MONTH_SHORT = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]

    NormalizedDate(String day, String month, String year) {
        //Normalize each field
        this.day = resolveDay(day);
        if (this.day == null) {
            throw new IllegalArgumentException(day + " is not a valid day.");
        }
        this.month = resolveMonth(month);
        if (this.month == null) {
            throw new IllegalArgumentException(month + "is not a valid month");
        }
        this.year = resolveYear(year);
        if (this.year == null) {
            throw new IllegalArgumentException(year + "is not a valid year");
        }
    }

    /*
     * YYYY/MM/DD format
     */
    @Override
    public String toString() {
        return year + "/" + month + "/" + day;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof NormalizedDate)) {
            return false;
        }

        NormalizedDate n2 = (NormalizedDate) obj;
        return toString().equals(n2.toString());
    }

    private String resolveDay(String day) {
        int dayInt = Integer.parseInt(day);
        if (dayInt > 0 && dayInt < 10) {
            return "0" + dayInt;
        } else if (dayInt >= 10 && dayInt <= 31) {
            return String.valueOf(dayInt);
        } else {
            return null;
        }
    }

    private String resolveMonth(String month) {
        int index = -1;
        if (month.length() > 3) {
            index = MONTH_LONG.findIndexOf {it == month};
        } else {
            index = MONTH_SHORT.findIndexOf {it == month}
        }
        if (index == -1) {
            return null;
        }

        String monthNum = String.valueOf(index + 1);

        if (monthNum.length() == 2) {
            return monthNum;
        } else if (monthNum.length() == 1) {
            return "0" + monthNum;
        } else {
            return null;
        }

    }

    private String resolveYear(String year) {
        year = year.replaceAll("[^0-9]", "");
        if (year.length() == 2) {
            return resolve2Year(year);
        } else if (year.length() == 4) {
            return resolve4Year(year);
        } else {
            return null;
        }
    }

    private String resolve2Year(String year) {
        int addYears = Integer.parseInt(year);
        if (addYears >= 0 && addYears <= 12) {
            return String.valueOf(2000 + addYears);
        } else if (addYears > 12) {
            return String.valueOf(1900 + addYears);
        } else {
            return null;
        }
    }

    private String resolve4Year(String year) {
        int yearInt = Integer.parseInt(year);
        if(yearInt >=1900 && yearInt <=2012)
        {
            return year;
        }
        else
        {
            return null;
        }
    }
}
