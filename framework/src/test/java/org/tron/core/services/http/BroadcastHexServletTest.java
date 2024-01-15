package org.tron.core.services.http;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.alibaba.fastjson.JSONObject;
import java.io.UnsupportedEncodingException;
import javax.annotation.Resource;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.tron.common.BaseTest;
import org.tron.core.Constant;
import org.tron.core.config.args.Args;

public class BroadcastHexServletTest extends BaseTest {
  @Resource
  private BroadcastHexServlet broadcastHexServlet;

  static {
    Args.setParam(
        new String[]{
            "--output-directory", dbPath(),
        }, Constant.TEST_CONF
    );
  }

  public MockHttpServletRequest createRequest(String contentType) {
    MockHttpServletRequest request = new MockHttpServletRequest();
    request.setMethod("POST");
    if (isNotEmpty(contentType)) {
      request.setContentType(contentType);
    }
    request.setCharacterEncoding("UTF-8");
    return request;
  }

  @Test
  public void testBroadCastTransaction() {
    String jsonParam = "{\n" +
        "    \"transaction\": \"0A8A010A0202DB2208C89D4811359A28004098A4E0A6B52D5A730802126F0A32747970652E676F6F6" +
        "76C65617069732E636F6D2F70726F746F636F6C2E5472616E736665724173736574436F6E747261637412390A073130303030303" +
        "11215415A523B449890854C8FC460AB602DF9F31FE4293F1A15416B0580DA195542DDABE288FEC436C7D5AF769D24206412418BF" +
        "3F2E492ED443607910EA9EF0A7EF79728DAAAAC0EE2BA6CB87DA38366DF9AC4ADE54B2912C1DEB0EE6666B86A07A6C7DF68F1F9D" +
        "A171EEE6A370B3CA9CBBB00\",\n" +
        "    \"visible\": true\n" +
        "}";
    MockHttpServletRequest request = createRequest("application/json");
    request.setContent(jsonParam.getBytes());
    MockHttpServletResponse response = new MockHttpServletResponse();
    broadcastHexServlet.doPost(request, response);
    try {
      String contentAsString = response.getContentAsString();
      JSONObject result = JSONObject.parseObject(contentAsString);
      assertEquals(result.getString("txid"),"38a0482d6d5a7d1439a50b848d68cafa7d904db48b82344f28765067a" +
          "5773e1d");
    } catch (UnsupportedEncodingException e) {
      fail(e.getMessage());
    }
  }
}
