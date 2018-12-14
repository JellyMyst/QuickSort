import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Main {
    public static void main(String[] args) {
        String fileString;
        try (BufferedReader r = new BufferedReader(new InputStreamReader(
                Objects.requireNonNull(Main.class.getClassLoader().getResourceAsStream("int_reverse_order.txt"))
        ))) {
            StringBuilder b = new StringBuilder();
            String line;
            while ((line = r.readLine()) != null) {
                b.append(line);
            }
            fileString = b.toString();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            return;
        }

        List<Integer> ints = new ArrayList<>();
        String[] temp = fileString.split(" ");
        for (String i : temp) {
            ints.add(Integer.valueOf(i));
        }
        List<Integer> sorted = new ArrayList<>(ints);
        sorted.sort(Integer::compareTo);

        Sorter<Integer> sorter = new Sorter<>(ints, "local[*]");
        List<Integer> serialSorted = sorter.serialSort();
        List<Integer> parallelSorted = sorter.parallelSort();

        assert sorted.equals(serialSorted);
        assert sorted.equals(parallelSorted);

        System.out.println("Serial time: " + sorter.getSerialTime() + System.lineSeparator()
                + "Parallel Time: " + sorter.getParallelTime());
    }
}
