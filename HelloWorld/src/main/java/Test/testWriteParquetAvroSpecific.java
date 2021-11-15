//package Test;
//
//import java.io.File;
//import java.util.ArrayList;
//import java.util.Comparator;
//
//public class testWriteParquetAvroSpecific {
//    final File folder = TEMPORARY_FOLDER.newFolder();
//
//    final List<Address> data = Arrays.asList(
//            new Address(1, "a", "b", "c", "12345"),
//            new Address(2, "p", "q", "r", "12345"),
//            new Address(3, "x", "y", "z", "12345")
//    );
//
//    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//	env.setParallelism(1);
//	env.enableCheckpointing(100);
//
//    DataStream<Address> stream = env.addSource(
//            new FiniteTestSource<>(data), TypeInformation.of(Address.class));
//
//
//
//
//
//}
