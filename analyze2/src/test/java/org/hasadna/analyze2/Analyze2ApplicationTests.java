package org.hasadna.analyze2;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class Analyze2ApplicationTests {

	@Test
	public void contextLoads() {
	}

	@Test
	public void testListOfDates() {
		ReadMakatFileImpl x = new ReadMakatFileImpl();
		// we don't call x.init()
		Assertions.assertThat( x.daysOfMonth(8, 2018) ).hasSize(31);
		Assertions.assertThat( x.daysOfMonth(2, 2018) ).hasSize(28);
		Assertions.assertThat( x.daysOfMonth(2, 2016) ).hasSize(29);
	}
}
