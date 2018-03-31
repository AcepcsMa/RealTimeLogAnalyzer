package controller;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dao.CategoryClickCountDAO;
import domain.CategoryClickCount;
import domain.QueryTime;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@EnableAutoConfiguration(exclude = { JacksonAutoConfiguration.class })
public class StatDisplayController {

    public static final String MAPPING_FILE = "/Users/marco/mapping.dat";
    private CategoryClickCountDAO categoryClickCountDAO = new CategoryClickCountDAO();
    private static Map<String, String> channelMapping = new HashMap<>();

    static {
        loadMapping();
    }

    @RequestMapping(value = "/stat", method = RequestMethod.GET)
    public ModelAndView showPage() {

        return new ModelAndView("stat");
    }

    @ResponseBody
    @RequestMapping(value = "/getClickCount", method = RequestMethod.POST)
    public List<CategoryClickCount> getClickCount(@RequestBody QueryTime queryTime) throws IOException {
        List<CategoryClickCount> clickCounts = categoryClickCountDAO.queryByTime(queryTime.toString(), null);
        for(CategoryClickCount clickCount : clickCounts) {
            clickCount.setCategoryName(channelMapping.get(clickCount.getCategoryName().substring(clickCount.getCategoryName().lastIndexOf("_") + 1)));
        }
        return clickCounts;
    }

    private static void loadMapping() {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(MAPPING_FILE));
            String line = null;
            StringBuilder mapping = new StringBuilder();
            while((line = reader.readLine()) != null) {
                mapping.append(line);
            }

            JsonParser parser = new JsonParser();
            JsonObject obj = (JsonObject) parser.parse(mapping.toString());

            for(Map.Entry<String, JsonElement> entry : obj.entrySet()) {
                channelMapping.put(entry.getKey(), entry.getValue().getAsString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
