import java.util.*;
class Check {
    static int checks = 0;
    static List<String> failures = new ArrayList<>();
    static void check(String name, Object got, Object want) {
        checks++;
        boolean ok = Objects.equals(got, want);
        if (!ok) failures.add(name + ": got " + got + ", want " + want);
    }
    static void report(String client) {
        System.out.println(client + ": " + checks + " checks, " + failures.size() + " failed");
        for (String f : failures) System.out.println("  FAIL " + f);
        System.exit(failures.isEmpty() ? 0 : 1);
    }
}
