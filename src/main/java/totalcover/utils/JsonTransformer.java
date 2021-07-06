package totalcover.utils;

import spark.ResponseTransformer;
import com.fasterxml.jackson.databind.ObjectMapper;
public class JsonTransformer implements ResponseTransformer {
    private static ObjectMapper om = new ObjectMapper();

    @Override
    public String render(Object model) throws Exception {
        // TODO Auto-generated method stub
        return om.writeValueAsString(model);
    }


}
