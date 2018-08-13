package pubmed;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class MeshLookup implements Serializable {
    Map<String, List<String>> treeNumbers = new HashMap<>();
    Map<String, String> descriptors = new HashMap<>();

    static String[] tokenize (String line) {
        int len = line.length();
        List<String> toks = new ArrayList<>();
        StringBuilder buf = new StringBuilder ();
        boolean quote = false;
        for (int i = 0; i < len; ++i) {
            char ch = line.charAt(i);
            switch (ch) {
            case ',':
                if (quote) {
                    buf.append(ch);
                }
                else {
                    toks.add(buf.toString());
                    buf.setLength(0);                    
                }
                break;
                
            case '"':
                quote = !quote;
                break;
                
            default:
                buf.append(ch);
            }
        }
        if (buf.length() > 0)
            toks.add(buf.toString());
        
        return toks.isEmpty() ? null : toks.toArray(new String[0]);
    }
    
    public MeshLookup () {
        try (BufferedReader br = new BufferedReader
             (new InputStreamReader
              (new GZIPInputStream (MeshLookup.class.getResourceAsStream
                                    ("/mesh2018_tree.csv.gz"))))) {
            for (String line; (line = br.readLine()) != null; ) {
                String[] toks = tokenize (line);
                //System.out.println(toks[0]+"\t"+toks[1]+"\t"+toks[2]);
                List<String> tr = treeNumbers.get(toks[0]);
                if (tr == null) {
                    treeNumbers.put(toks[0], tr = new ArrayList<>());
                }
                tr.add(toks[2]);
                descriptors.put(toks[2], toks[1]); // tree number -> name
            }
            System.err.println("## "+treeNumbers.size()
                               +" descriptors with tree numbers loaded!");
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    
    public String[] getTreeNumbers (String id) {
        List<String> tr = treeNumbers.get(id);
        return tr != null ? tr.toArray(new String[0]) : null;
    }

    public String getDescriptorNameFromTreeNumber (String treeNumber) {
        return descriptors.get(treeNumber);
    }
}
