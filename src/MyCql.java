import ch.qos.logback.core.joran.spi.JoranException;
import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.model.RowImpl;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import org.apache.log4j.Logger;
import java.util.Iterator;
import java.lang.NoClassDefFoundError;
public class MyCql
{
    public static final String CLUSTERNAME="Test Cluster";
    public static final String HOSTLP="localHost:7199";
    public static final String KEYSPACE="mycql";

    Keyspace keySpace = null;
    Logger logger=null ;
    Cluster cluster;
    public static void main(String[] args) throws JoranException {
       MyCql gt=new MyCql();
       gt.config();
       gt.read();
    }
    public void config() throws JoranException {
        try {

            cluster = HFactory.getOrCreateCluster(CLUSTERNAME, HOSTLP);
            keySpace = HFactory.createKeyspace(KEYSPACE, cluster);
return;
        }
        catch (NoClassDefFoundError e){
            System.out.println("Failed to make connection!");
            logger.error("Exception " + e);
            return;
        }
    }
    public void read() throws JoranException {
        if (null != cluster && null != keySpace) {
            CqlQuery cqlQuery = new CqlQuery(keySpace, StringSerializer.get(), StringSerializer.get(),StringSerializer.get());
            cqlQuery.setQuery("select * from category");
            QueryResult  result = cqlQuery.execute();
            CqlRows rows = (CqlRows) result.get();
            for (int i = 0; i < rows.getCount(); i++) {
                RowImpl row = (RowImpl) rows.getList().get(i);
                System.out.println("Row key = " + row.getKey());
                for (Iterator iterator = row.getColumnSlice().getColumns().iterator(); iterator.hasNext(); ) {
                    HColumn column = (HColumn) iterator.next();
                    System.out.println("Column name = " + column.getName().toString());
                    System.out.println("Column name = " + column.getName().toString());
                }
            }

        }
    }
}
