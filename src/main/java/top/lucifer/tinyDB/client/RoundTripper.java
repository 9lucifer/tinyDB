package top.lucifer.tinyDB.client;

import top.lucifer.tinyDB.transport.Package;
import top.lucifer.tinyDB.transport.Packager;

public class RoundTripper {
    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }
}
