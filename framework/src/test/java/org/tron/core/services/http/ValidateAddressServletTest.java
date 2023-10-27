package org.tron.core.services.http;

import static org.junit.Assert.fail;

import com.alibaba.fastjson.JSONObject;
import java.io.UnsupportedEncodingException;
import javax.annotation.Resource;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.tron.common.BaseTest;
import org.tron.core.Constant;
import org.tron.core.config.args.Args;

public class ValidateAddressServletTest extends BaseTest {

  @Resource
  private ValidateAddressServlet validateAddressServlet;

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
    String jsonParam = "{\"address\": \"a0abd4b9367799eaa3197fecb144eb71de1e049abc\"}";
    MockHttpServletRequest request = createRequest("POST","application/json");
    request.setContent(jsonParam.getBytes());
    MockHttpServletResponse response = new MockHttpServletResponse();
    validateAddressServlet.doPost(request, response);
    try {
      String contentAsString = response.getContentAsString();
      JSONObject result = JSONObject.parseObject(contentAsString);
      Assert.assertEquals(result.getBoolean("result"), true);
      Assert.assertEquals(result.getString("message"), "Hex string format");
    } catch (UnsupportedEncodingException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testDoGet() {
    MockHttpServletRequest request = createRequest("GET",
        "application/x-www-form-urlencoded");
    request.setParameter("address","a0abd4b9367799eaa3197fecb144eb71de1e049abc");
    MockHttpServletResponse response = new MockHttpServletResponse();
    validateAddressServlet.doGet(request, response);
    try {
      String contentAsString = response.getContentAsString();
      JSONObject result = JSONObject.parseObject(contentAsString);
      Assert.assertEquals(result.getBoolean("result"), true);
      Assert.assertEquals(result.getString("message"), "Hex string format");
    } catch (UnsupportedEncodingException e) {
      fail(e.getMessage());
    }
  }
}