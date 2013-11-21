package assignment2;


public class Utils {

    public static String nullsafeLowercase(String pStr) {
        if (pStr == null) {
            return null;
        }
        return pStr.toLowerCase();
    }

    public static String removeSpecialChars(CharSequence pSeq) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < pSeq.length(); i++) {
            char c = pSeq.charAt(i);
            if (c == ',' || c == '.' || c == '"') {
                // do nothing you son of a bitch
            } else if (c == '-') {
                sb.append(" ");
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

}
