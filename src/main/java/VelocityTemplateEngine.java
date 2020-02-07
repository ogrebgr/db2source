import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;


public class VelocityTemplateEngine implements TemplateEngine {
    private final VelocityEngine velocityEngine;
    private final VelocityContext velocityContext;
    private final String templatePathPrefix;


    public VelocityTemplateEngine(VelocityEngine velocityEngine, String templatePathPrefix) {
        this.velocityEngine = velocityEngine;
        velocityContext = new VelocityContext();
        this.templatePathPrefix = templatePathPrefix;
    }


    @Override
    public void assign(String varName, Object object) {
        velocityContext.put(varName, object);
    }


    @Override
    public String render(String templateName) {
        assign("_dir", templatePathPrefix);
        StringWriter sw = new StringWriter();
        Template t = velocityEngine.getTemplate(templatePathPrefix + templateName, "UTF-8");
        t.merge(velocityContext, sw);
        return sw.toString();
    }
}
