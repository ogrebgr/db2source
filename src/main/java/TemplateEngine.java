public interface TemplateEngine {
    /**
     * Assign value to a variable name
     *
     * @param varName Variable name
     * @param object  Value
     */
    void assign(String varName, Object object);

    /**
     * Renders a template to string
     *
     * @param templateName Template name
     * @return Rendered string, usually HTML
     */
    String render(String templateName);
}
