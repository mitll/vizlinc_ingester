import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses string and returns a NormalizedDate object corresponding to the input
 * string.
 *
 * @author JO21372
 * adapted for Groovy: dhalbert
 */
class DateNormalizer {
    static final LONG_MONTH = '(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)'
    static final SHORT_MONTH = '(?:ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)'
    static final DAY_OF_MONTH = '(?:[1-9]|0[1-9]|[1-2][0-9]|30|31)'
    static final DAY_OF_MONTH_WORDS = '(?:uno|primero|segundo|dos|tres|cuatro|cinco|seis|siete|ocho|nuevo|diez|once|doce|trece|catorce|quince|dieciseis|diecisiete|dieciocho|diecinueve|veinte|veintiuno|veintidos|veintitres|veinticuatro|veinticinco|veintiseis|veintisiete|veintiocho|veintinueve|treinta|treinta y uno)'
    //Years in YYYY format from 1900 to 2012
    static final YEAR4 = /(?:1\.?9[0-9][0-9]|2\.?0[01][0-9])/
    static final YEAR2 = '(?:[0-9][0-9])'

    static normalizeString(String date) {
        String result = date.toLowerCase();
        result = result.replaceAll(/\s+/, " ");
        return result;
    }

    static NormalizedDate normalize(String date) {
        String nString = normalizeString(date);
        NormalizedDate nDate = null;

        String regex1 = /($DAY_OF_MONTH) (?:de )?($LONG_MONTH) (?:de |del |del año )?($YEAR4)/
        String day = null;
        String month = null;
        String year = null;

        Pattern p1 = Pattern.compile(regex1);
        Matcher m1 = p1.matcher(nString);
        if (m1.find()) {
            day = m1.group(1);
            month = m1.group(2);
            year = m1.group(3);

            nDate = new NormalizedDate(day, month, year);
            return nDate;
        }

        String regex2 = /(?:el día )?($DAY_OF_MONTH)([\/-])($SHORT_MONTH|$LONG_MONTH)\2($YEAR4|$YEAR2)/
        Pattern p2 = Pattern.compile(regex2);
        Matcher m2 = p2.matcher(nString);
        if (m2.find()) {
            day = m2.group(1);
            // group(2) is the / or -
            month = m2.group(3);
            year = m2.group(4);
            nDate = new NormalizedDate(day, month, year);
            return nDate;
        }

        //Month first
        String regex3 = /($LONG_MONTH) ($DAY_OF_MONTH) del? ($YEAR4)/
        Pattern p3 = Pattern.compile(regex3);
        Matcher m3 = p3.matcher(nString);
        if(m3.find()) {
            day = m3.group(2);
            month = m3.group(1);
            year = m3.group(3);
            nDate = new NormalizedDate(day, month, year);
            return nDate;
        }

        println(date + " -> " + nDate);
        return nDate;
    }
}