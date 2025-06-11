package top.lucifer.tinyDB.client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.cli.*;
import top.lucifer.tinyDB.transport.Encoder;
import top.lucifer.tinyDB.transport.Packager;
import top.lucifer.tinyDB.transport.Transporter;

public class Launcher {
    public static void main(String[] args) throws UnknownHostException, IOException, ParseException {
        Options options = new Options();
        options.addOption("username", true, "-DBA name");
        options.addOption("password", true, "-DBA password");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options,args);
        if(cmd.hasOption("username") && cmd.hasOption("password")) {
            String username = cmd.getOptionValue("username");
            String password = cmd.getOptionValue("password");
            if(username.equals("root") && password.equals("123456")) {
                Socket socket = new Socket("127.0.0.1", 9999);
                Encoder e = new Encoder();
                Transporter t = new Transporter(socket);
                Packager packager = new Packager(t, e);
                System.out.println("=====================================");
                System.out.println("=====Welcome "+username+" to use TinyDB!=====");
                System.out.println("====database init successfully!======");
                System.out.println("=====================================");
                Client client = new Client(packager);
                Shell shell = new Shell(client);
                shell.run();
            }else {
                System.out.println("==============================");
                System.out.println("Invalid username or password!");
                System.out.println("username:"+username);
                System.out.println("password:"+password);
                System.out.println("==============================");

            }
        }else {
            System.out.println("==============================");
            System.out.println("Invalid username or password!");
            System.out.println("==============================");

        }

    }
}
