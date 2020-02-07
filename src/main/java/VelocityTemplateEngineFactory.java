import org.apache.velocity.app.VelocityEngine;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class VelocityTemplateEngineFactory {
    private final VelocityEngine velocityEngine;
    private final String templatePathPrefix;


    public VelocityTemplateEngineFactory(String templatePathPrefix, Map<String, String> additionalSettings) {
        velocityEngine = new VelocityEngine();
        Properties properties = new Properties();
        properties.setProperty("resource.loader", "class");
        properties.setProperty(
                "class.resource.loader.class",
                "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");

        for (String key : additionalSettings.keySet()) {
            properties.setProperty(key, additionalSettings.get(key));
        }

        velocityEngine.init(properties);

        this.templatePathPrefix = templatePathPrefix;
    }


    public VelocityTemplateEngineFactory(String templatePathPrefix) {
        this(templatePathPrefix, new HashMap<>());
    }


    public TemplateEngine createNew() {
        return new VelocityTemplateEngine(velocityEngine, templatePathPrefix);
    }
}
