package org.tron.core.services.http;

import static org.junit.Assert.assertTrue;
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

public class GetBlockServletTest extends BaseTest {

  @Resource
  private GetBlockServlet getBlockServlet;

  static {
    Args.setParam(
        new String[]{
            "--output-directory", dbPath(),
        }, Constant.TEST_CONF
    );
  }

  public MockHttpServletRequest createRequest(String method, String contentType) {
    MockHttpServletRequest request = new MockHttpServletRequest(method,null);
    request.setContentType(contentType);
    request.setCharacterEncoding("UTF-8");
    return request;
  }

  @Test
  public void testDoPost() {
    String jsonParam = "{\"visible\": true}";
    MockHttpServletRequest request = createRequest("POST","application/json");
    request.setContent(jsonParam.getBytes());
    MockHttpServletResponse response = new MockHttpServletResponse();
    getBlockServlet.doPost(request, response);
    try {
      String contentAsString = response.getContentAsString();
      JSONObject result = JSONObject.parseObject(contentAsString);
      assertTrue(result.containsKey("blockID"));
    } catch (UnsupportedEncodingException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testDoGet() {
    MockHttpServletRequest request = createRequest("GET",
        "application/x-www-form-urlencoded");
    try {
      request.setParameter("visible","true");
      MockHttpServletResponse response = new MockHttpServletResponse();
      getBlockServlet.doGet(request, response);
      String contentAsString = response.getContentAsString();
      JSONObject result = JSONObject.parseObject(contentAsString);
      assertTrue(result.containsKey("blockID"));
    } catch (UnsupportedEncodingException e) {
      fail(e.getMessage());
    }
  }

}
