import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class COVIDCasesMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String line = value.toString().trim();
        
        // Skip header row (السطر الأول في الملف)
        if (line.startsWith("Province/State") || line.startsWith("Province")) {
            return;
        }
        
        // لو السطر فاضي، نتخطاه
        if (line.isEmpty()) {
            return;
        }
        
        String[] fields = line.split(",");
        
        // التأكد إن فيه بيانات كافية
        if (fields.length >= 5) {
            
            // جيب اسم الدولة (العمود الثاني في ملف Johns Hopkins)
            String country = fields[1].trim();
            
            // لو الدولة فاضية، استخدم Province/State
            if (country.isEmpty()) {
                country = fields[0].trim();
            }
            
            // لو لسه فاضي، نتخطى
            if (country.isEmpty()) {
                return;
            }
            
            // حساب إجمالي الحالات (مجموع الأعمدة من التاريخ)
            int totalCases = 0;
            for (int i = 4; i < fields.length; i++) {
                try {
                    String val = fields[i].trim();
                    if (!val.isEmpty()) {
                        totalCases += Integer.parseInt(val);
                    }
                } catch (NumberFormatException e) {
                    // نتخطى الخانات غير الرقمية
                }
            }
            
            
            // if (totalCases == 0) return;
            
            String covidData = "cases=" + totalCases;
            
            outputKey.set(country);
            outputValue.set("C|" + covidData);
            
            context.write(outputKey, outputValue);
        }
    }
}