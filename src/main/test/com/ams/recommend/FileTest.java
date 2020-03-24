package com.ams.recommend;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class FileTest {
    public static void main(String[] args) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File("root@ams:~/data/article.txt"));
        while(scanner.hasNextLine()){
            System.out.println(scanner.nextLine());
        }
    }
}
