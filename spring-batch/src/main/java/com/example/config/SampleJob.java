package com.example.config;

import java.io.IOException;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.adapter.ItemWriterAdapter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import com.example.listener.SkipListener;
import com.example.listener.SkipListenerImpl;
import com.example.model.StudentCsv;
import com.example.model.StudentJdbc;
import com.example.model.StudentJson;
import com.example.model.StudentResponse;
import com.example.model.StudentXml;
import com.example.processor.FirstItemProcessor;
import com.example.reader.FirstItemReader;
import com.example.service.StudentService;
import com.example.writer.FirstItemWriter;

@Configuration
public class SampleJob {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private FirstItemReader firstItemReader;

	@Autowired
	private FirstItemProcessor firstItemProcessor;

	@Autowired
	private FirstItemWriter firstItemWriter;

	/*
	 * Una sola fuente de datos
	 * 
	 * @Autowired private DataSource dataSource;
	 */

	@Autowired
	@Qualifier("dataSource")
	private DataSource dataSource;

	@Autowired
	@Qualifier("universityDatasource")
	private DataSource universityDatasource;

	@Autowired
	StudentService studentService;
	
	@Autowired
	private SkipListener skipListener;
	
	@Autowired
	private SkipListenerImpl skipListenerImpl;

	// Trabajo para compremender el paso orientado a trozos.
	@Bean
	public Job chunkJob() {
		return jobBuilderFactory.get("Chunk Job").incrementer(new RunIdIncrementer()).start(firstChunkStep()).build();
	}

	private Step firstChunkStep() {
		return stepBuilderFactory.get("First Chunk Step").<StudentCsv, StudentJson>chunk(3)
				.reader(flatFileItemReader())
				// .reader(jsonItemReader())
				// .reader(staxEventItemReader())
				// .reader(jdbcCursorItemReader())
				// .reader(itemReaderAdapter())
				.processor(firstItemProcessor)
				// .writer(firstItemWriter)
				// .writer(flatFileItemWriter())
				 .writer(jsonFileItemWriter())
				// .writer(staxEventItemWriter())
				// .writer(jdbcBatchItemWriter())
				// .writer(jdbcBatchItemWriter1())
				//.writer(itemWriterAdapter())
				 .faultTolerant()
				// Se pueden lanzar excepciones en particular para ser controladas con un try catch
				 // .skip(FlatFileParseException.class)
				 // .skip(NullPointerException.class)
				 // Todas las excepciones
				 .skip(Throwable.class)
				  .skipLimit(100)
				 // .skipPolicy(new AlwaysSkipItemSkipPolicy())
				 // Mecanismo de reintentar (valido para processor y writter)
				 .retryLimit(1)
				 .retry(Throwable.class)
				 //.listener(skipListener)
				 .listener(skipListenerImpl)
				 .build();
	}

	public FlatFileItemReader<StudentCsv> flatFileItemReader() {

		FlatFileItemReader<StudentCsv> flatFileItemReader = new FlatFileItemReader<StudentCsv>();

		flatFileItemReader.setResource(new FileSystemResource(
				"C:\\Users\\Usuario\\OneDrive\\Cursos\\Curso spring batch\\Workspace\\spring-batch\\InputFiles\\students.csv"));

		flatFileItemReader.setLineMapper(new DefaultLineMapper<StudentCsv>() {
			{
				setLineTokenizer(new DelimitedLineTokenizer() {
					{
						// Por defecto en DelimitedLineTokenizer viene ","
						// Encabezados de columna
						setNames("ID", "First Name", "Last Name", "Email");
						// Método para cambiar el delimitador.
						// setDelimiter("|");
					}
				});

				// Leer cada línea del CSV y obtener un objeto de nuestra clase modelo.
				setFieldSetMapper(new BeanWrapperFieldSetMapper<StudentCsv>() {
					{
						setTargetType(StudentCsv.class);
					}
				});

			}
		});

		// Saltarse la primera linea que hace referencia al nombre de las columnas.
		flatFileItemReader.setLinesToSkip(1);

		return flatFileItemReader;

	}

	public JsonItemReader<StudentJson> jsonItemReader() {

		JsonItemReader<StudentJson> jsonItemReader = new JsonItemReader<StudentJson>();

		jsonItemReader.setResource(new FileSystemResource(
				"C:\\Users\\Usuario\\OneDrive\\Cursos\\Curso spring batch\\Workspace\\spring-batch\\InputFiles\\students.json"));
		jsonItemReader.setJsonObjectReader(new JacksonJsonObjectReader<>(StudentJson.class));
		// Para leer los 8 primeros objetos del JSON
		jsonItemReader.setMaxItemCount(8);
		// Para que empiece por el 3 objeto.
		jsonItemReader.setCurrentItemCount(2);

		return jsonItemReader;

	}

	public StaxEventItemReader<StudentXml> staxEventItemReader() {

		StaxEventItemReader<StudentXml> staxEventItemReader = new StaxEventItemReader<StudentXml>();

		staxEventItemReader.setResource(new FileSystemResource(
				"C:\\Users\\Usuario\\OneDrive\\Cursos\\Curso spring batch\\Workspace\\spring-batch\\InputFiles\\students.xml"));
		staxEventItemReader.setFragmentRootElementName("student");
		staxEventItemReader.setUnmarshaller(new Jaxb2Marshaller() {
			{
				setClassesToBeBound(StudentXml.class);
			}
		});

		return staxEventItemReader;
	}

	public JdbcCursorItemReader<StudentJdbc> jdbcCursorItemReader() {

		JdbcCursorItemReader<StudentJdbc> jdbcCursorItemReader = new JdbcCursorItemReader<StudentJdbc>();

		jdbcCursorItemReader.setDataSource(universityDatasource);
		jdbcCursorItemReader.setSql("select id, first_name as firstName, last_name as lastName, email from student");
		jdbcCursorItemReader.setRowMapper(new BeanPropertyRowMapper<StudentJdbc>() {
			{
				setMappedClass(StudentJdbc.class);
			}
		});

		// Se salta las dos primeras filas ....
		// jdbcCursorItemReader.setCurrentItemCount(2);

		// Leer hasta la 8 ...
		// jdbcCursorItemReader.setMaxItemCount(8);

		return jdbcCursorItemReader;

	}

	/*
	 * public ItemReaderAdapter<StudentResponse> itemReaderAdapter(){
	 * 
	 * ItemReaderAdapter<StudentResponse> itemReaderAdapter = new
	 * ItemReaderAdapter<StudentResponse> ();
	 * 
	 * itemReaderAdapter.setTargetObject(studentService);
	 * itemReaderAdapter.setTargetMethod("getStudent");
	 * itemReaderAdapter.setArguments(new Object[] {1L , "Test"});
	 * 
	 * return itemReaderAdapter;
	 * 
	 * }
	 */

	// CSV
	public FlatFileItemWriter<StudentJdbc> flatFileItemWriter() {

		FlatFileItemWriter<StudentJdbc> flatFileItemWriter = new FlatFileItemWriter<StudentJdbc>();

		flatFileItemWriter.setResource(new FileSystemResource(
				"C:\\Users\\Usuario\\OneDrive\\Cursos\\Curso spring batch\\Workspace\\spring-batch\\OutputFiles\\students.csv"));

		flatFileItemWriter.setHeaderCallback(new FlatFileHeaderCallback() {

			@Override
			public void writeHeader(Writer writer) throws IOException {
				writer.write("Id,First Name,Last Name,Email");
			}
		});

		flatFileItemWriter.setLineAggregator(new DelimitedLineAggregator<StudentJdbc>() {
			{
				// setDelimiter("|");
				setFieldExtractor(new BeanWrapperFieldExtractor<StudentJdbc>() {
					{
						setNames(new String[] { "id", "firstName", "lastName", "email" });
					}
				});
			}
		});

		flatFileItemWriter.setFooterCallback(new FlatFileFooterCallback() {

			public void writeFooter(Writer writer) throws IOException {
				writer.write("Created @" + LocalDate.now());
			}
		});

		return flatFileItemWriter;

	}

	// JSON
	public JsonFileItemWriter<StudentJson> jsonFileItemWriter() {

		JsonFileItemWriter<StudentJson> jsonFileItemWriter = new JsonFileItemWriter<StudentJson>(new FileSystemResource(
				"C:\\Users\\Usuario\\OneDrive\\Cursos\\Curso spring batch\\Workspace\\spring-batch\\OutputFiles\\students.json"),
				new JacksonJsonObjectMarshaller<StudentJson>()) {
			@Override
			public String doWrite(List<? extends StudentJson> items) {
				items.stream().forEach(item -> {
					if(item.getId() == 3) {
						System.out.println("Inside JsonFileItemWriter");
						throw new NullPointerException();
					}
				});
				return super.doWrite(items);
			}
		};
		
		

		return jsonFileItemWriter;

	}

	// XML
	public StaxEventItemWriter<StudentJdbc> staxEventItemWriter() {

		StaxEventItemWriter<StudentJdbc> staxEventItemWriter = new StaxEventItemWriter<StudentJdbc>();

		staxEventItemWriter.setResource(new FileSystemResource(
				"C:\\Users\\Usuario\\OneDrive\\Cursos\\Curso spring batch\\Workspace\\spring-batch\\OutputFiles\\students.xml"));
		staxEventItemWriter.setRootTagName("students");
		staxEventItemWriter.setMarshaller(new Jaxb2Marshaller() {
			{
				setClassesToBeBound(StudentJdbc.class);
			}
		});

		return staxEventItemWriter;

	}

	// MySQL
	@Bean
	public JdbcBatchItemWriter<StudentCsv> jdbcBatchItemWriter() {

		JdbcBatchItemWriter<StudentCsv> jdbcBatchItemWriter = new JdbcBatchItemWriter<StudentCsv>();
		jdbcBatchItemWriter.setDataSource(universityDatasource);
		jdbcBatchItemWriter.setSql("insert into student(id, first_name, last_name, email)"
				+ "values (:id, :firstName, :lastName, :email)");
		jdbcBatchItemWriter
				.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<StudentCsv>());
		return jdbcBatchItemWriter;
	}

	// MySQL otra forma
	@Bean
	public JdbcBatchItemWriter<StudentCsv> jdbcBatchItemWriter1() {

		JdbcBatchItemWriter<StudentCsv> jdbcBatchItemWriter = new JdbcBatchItemWriter<StudentCsv>();
		jdbcBatchItemWriter.setDataSource(universityDatasource);
		jdbcBatchItemWriter.setSql("insert into student(id, first_name, last_name, email)" + "values (?,?,?,?)");
		jdbcBatchItemWriter.setItemPreparedStatementSetter(new ItemPreparedStatementSetter<StudentCsv>() {

			@Override
			public void setValues(StudentCsv item, PreparedStatement ps) throws SQLException {
				ps.setLong(1, item.getId());
				ps.setString(2, item.getFirstName());
				ps.setString(3, item.getLastName());
				ps.setString(4, item.getEmail());
			}
		});
		return jdbcBatchItemWriter;
	}

	public ItemWriterAdapter<StudentCsv> itemWriterAdapter() {

		ItemWriterAdapter<StudentCsv> itemWriterAdapter = new ItemWriterAdapter<StudentCsv>();

		itemWriterAdapter.setTargetObject(studentService);
		// Sprin batch se encarga de pasar el objeto ...
		itemWriterAdapter.setTargetMethod("restCallToCreateStudent");

		return itemWriterAdapter;

	}

}
