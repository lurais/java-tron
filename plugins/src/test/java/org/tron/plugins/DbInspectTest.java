package org.tron.plugins;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class DbInspectTest extends DbTest {

  @Test
  public void testRun() throws IOException {
    String[] args = new String[] {"db", "ins","-fn" , INPUT_DIRECTORY};
    Assert.assertEquals(0, cli.execute(args));
  }

}