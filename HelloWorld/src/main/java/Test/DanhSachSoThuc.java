package Test;

import java.util.ArrayList;
import java.util.Scanner;

public class DanhSachSoThuc {
    private ArrayList <Double> list = new ArrayList<>();
    public void nhap(){
        System.out.println("Nhap danh sach cac so thuc: ");
        int yes=1; //1 cai co`
        Scanner s = new Scanner(System.in);
        do{
            System.out.println("Nhap so: ");
            double num = s.nextDouble();
            list.add(num);
            System.out.println("Ban co muon nhap tiep (1: tiep, 0: thoat): ");
            yes = s.nextInt();
        } while (yes == 1);
    }
}
