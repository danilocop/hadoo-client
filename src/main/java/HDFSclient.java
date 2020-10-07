import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

public class HDFSclient {

   public static void main (String[] args) throws IOException {

      System.setProperty("java.security.krb5.realm", "SUPPORTLAB.CLOUDERA.COM");
      System.setProperty("java.security.krb5.kdc", "c138-node1.supportlab.cloudera.com");

      Configuration conf = new Configuration();

      String hdfsPath = "/dperez/";

      conf.set("hadoop.security.authentication", "kerberos");
      conf.set("hadoop.security.authorization", "true");

      conf.set("fs.defaultFS", "hdfs://c138-node2.supportlab.cloudera.com:8020");
      conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());

   //   conf.set("dfs.client.use.datanode.hostname", "true");

   //   conf.set("dfs.namenode.kerberos.principal.pattern", "hddfs/_@SUPPORTLAB.CLOUDERA.COM");

      System.setProperty("java.security.krb5.conf","/Users/dperez/Documents/projetos/hdfs_client/danilo/src/krb5.conf");

      String principal = System.getProperty("kerberosPrincipal","dperez@SUPPORTLAB.CLOUDERA.COM");
      String keytabLocation = System.getProperty("kerberosKeytab","/Users/dperez/Documents/projetos/hdfs_client/danilo/src/main/resources/dperez.keytab");

      UserGroupInformation.setConfiguration(conf);
      try {
         UserGroupInformation.loginUserFromKeytab(principal, keytabLocation);
      } catch (IOException e) {
         e.printStackTrace();
      }

      FileSystem fs = FileSystem.get(conf);
      Path path = new Path(hdfsPath);

      RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, true);
      while(files.hasNext()) {
         LocatedFileStatus file = files.next();
         System.out.println(file);

         if(file.isFile() && file.getPath().getName().equalsIgnoreCase("testfile.txt")) {
            FSDataOutputStream os = fs.append(file.getPath());
            os.write("Some test\n".getBytes());
            os.close();
         }
      }

   }

}
