package DroppableNestedColumns;

import java.util.List;

/**
 * @author vdokku
 */
public class Pojo {


    private String str;
    private Integer number;
    private List<String> strList;
    private Pojo2 pojo2;

    public String getStr() {
        return str;
    }

    public Integer getNumber() {
        return number;
    }

    public List<String> getStrList() {
        return strList;
    }

    public Pojo2 getPojo2() {
        return pojo2;
    }


}
