
import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        try {
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
