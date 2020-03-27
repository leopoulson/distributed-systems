
import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        try {
            System.out.println(args[0]);
            System.out.println(args[1]);
            Network.main(args);
        }
        catch (IOException e) {
            System.out.println("io exception");
        }
        catch (InterruptedException e) {
            System.out.println("interrupted exception");
        }
    }
}
