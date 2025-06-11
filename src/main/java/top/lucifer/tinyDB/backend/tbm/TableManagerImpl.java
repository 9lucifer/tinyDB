package top.lucifer.tinyDB.backend.tbm;

import top.lucifer.tinyDB.backend.dm.DataManager;
import top.lucifer.tinyDB.backend.utils.Parser;
import top.lucifer.tinyDB.backend.vm.VersionManager;
import top.lucifer.tinyDB.common.Error;
import top.lucifer.tinyDB.backend.parser.statement.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TableManagerImpl implements TableManager {
    VersionManager vm;
    DataManager dm;
    private Booter booter;
    private Map<String, Table> tableCache;
    private Map<Long, List<Table>> xidTableCache;
    private Lock lock;
    TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.xidTableCache = new HashMap<>();
        lock = new ReentrantLock();
        loadTables();
    }

    private void loadTables() {
        long uid = firstTableUid();
        while(uid != 0) {
            Table tb = Table.loadTable(this, uid);
            uid = tb.nextUid;
            tableCache.put(tb.name, tb);
        }
    }

    private long firstTableUid() {
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    private void updateFirstTableUid(long uid) {
        byte[] raw = Parser.long2Byte(uid);
        booter.update(raw);
    }

    @Override
    public BeginRes begin(Begin begin) {
        BeginRes res = new BeginRes();
        int level = begin.isRepeatableRead?1:0;
        res.xid = vm.begin(level);
        res.result = "begin".getBytes();
        return res;
    }
    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid);
        return "commit".getBytes();
    }
    @Override
    public byte[] abort(long xid) {
        vm.abort(xid);
        return "abort".getBytes();
    }
    @Override
    public byte[] show(long xid) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            // 添加表头
            sb.append("Table name\n");
            sb.append("-----------\n");

            // 添加所有缓存的表
            for (Table tb : tableCache.values()) {

                sb.append(tb.toString()).append("\n");
            }

            // 添加当前事务创建的表（如果有）
            List<Table> t = xidTableCache.get(xid);
            if(t != null) {
                for (Table tb : t) {
                    sb.append(tb.toString()).append("\n");
                }
            }

            // 如果没有表，显示提示信息
            if(tableCache.isEmpty() && (t == null || t.isEmpty())) {
                sb.append("No tables found.\n");
            }

            return sb.toString().getBytes();
        } finally {
            lock.unlock();
        }
    }
//    https://www.bilibili.com/video/BV1VN411J7S7/
    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            if(tableCache.containsKey(create.tableName)) {
                throw Error.DuplicatedTableException;
            }
            Table table = Table.createTable(this, firstTableUid(), xid, create);
            updateFirstTableUid(table.uid);
            tableCache.put(create.tableName, table);
            if(!xidTableCache.containsKey(xid)) {
                xidTableCache.put(xid, new ArrayList<>());
            }
            xidTableCache.get(xid).add(table);
            return ("create " + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }
    }
    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        lock.lock();
        Table table = tableCache.get(insert.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        table.insert(xid, insert);
        return "insert".getBytes();
    }
    @Override
    public byte[] read(long xid, Select read) throws Exception {
        lock.lock();
        Table table = tableCache.get(read.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        return table.read(xid, read).getBytes();
    }
    @Override
    public byte[] update(long xid, Update update) throws Exception {
        lock.lock();
        Table table = tableCache.get(update.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.update(xid, update);
        return ("update " + count).getBytes();
    }
    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        lock.lock();
        Table table = tableCache.get(delete.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.delete(xid, delete);
        return ("delete " + count).getBytes();
    }

    // 新增方法：打印所有表及其数据
    public byte[] showContent(long xid) throws Exception {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();

            // 获取所有表
            List<Table> allTables = new ArrayList<>(tableCache.values());
            List<Table> xidTables = xidTableCache.get(xid);
            if(xidTables != null) {
                allTables.addAll(xidTables);
            }

            if(allTables.isEmpty()) {
                return "No tables found.".getBytes();
            }

            // 遍历每个表
            for(Table tb : allTables) {
                sb.append("===== Table: ").append(tb.name).append(" =====\n");

                // 打印表结构
                sb.append("Schema: ").append(tb.toString()).append("\n");

                // 打印表数据
                Select select = new Select();
                select.tableName = tb.name;
                String content = tb.read(xid, select);

                if(content.trim().isEmpty()) {
                    sb.append("(No data)\n");
                } else {
                    sb.append("Data:\n").append(content);
                }
                sb.append("\n");
            }

            return sb.toString().getBytes();
        } finally {
            lock.unlock();
        }
    }
}
