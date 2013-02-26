package testPackage; // Always use packages. Never use default package.

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** Very simplistic servlet that generates plain text.
 *  Uses the @WebServlet annotation that is supported by
 *  Tomcat 7 and other servlet 3.0 containers. 
 *  
 *  From <a href="http://courses.coreservlets.com/Course-Materials/">the
 *  coreservlets.com tutorials on servlets, JSP, Struts, JSF 2.x, 
 *  Ajax, jQuery, GWT, Spring, Hibernate/JPA, Hadoop, and Java programming</a>.
 */

@WebServlet("/session")
public class SessionServlet extends HttpServlet {
  private static long nextSessionID = 0;
  private static final String COOKIE_NAME = "CS5300_WJK56_DRM237_BDT25";
  private String serverName;
  private ConcurrentHashMap<String, SessionData> sessionMap;
	
  public SessionServlet(String serverName) {
	  super();
	  sessionMap = new ConcurrentHashMap<String, SessionServlet.SessionData>();
	  if(serverName == null)
		  this.serverName = "Server 1";
	  else
		  this.serverName = serverName;
  }
  
  @Override
  public void doGet(HttpServletRequest request,
                    HttpServletResponse response)
      throws ServletException, IOException {
	  
	  Cookie[] cookies = request.getCookies();
	  String cmd = request.getParameter("command");
	  
	  if(cookies == null) {
		  Cookie cookie = newSessionState(request, response);
		  refresh(request, response, cookie);
	  	  return;
	  }
	  else {
		  for(Cookie cookie : cookies) {
			  if(COOKIE_NAME.equals(cookie.getName())) {
				  if(cmd == null || cmd.equals("refresh")) {
					  refresh(request, response, cookie);
					  return;
				  } else if(cmd.equals("replace")) {
					  replace(request, response, cookie);
				  	  refresh(request, response, cookie);
				  	  return;
				  } else if(cmd.equals("logout")) {
					  logout(request, response);
					  return;
				  }
			  }
		  }
	  }
	  
	  //If we got here than there are cookies, but none with
	  //the name COOKIE_NAME, so we need to create a new session
	  
	  Cookie cookie = newSessionState(request, response);
	  refresh(request, response, cookie);
  }
				
		 

private void logout(HttpServletRequest request, HttpServletResponse response, String sessionID) {
	// remove session from session table
	sessionMap.remove(sessionID);
	response.setContentType("text/html");
	try {
		PrintWriter out = response.getWriter();
		out.println("<!DOCTYPE html>\n" +
				"<html>\n" +
				"<head><title>Bye!</title></head>\n" +
				"<body>\n" +
				"<h1>Bye!</h1>\n" +
				"</body></html>");
	} catch (IOException e) {
		// TODO do nothing?
	}
}

private Cookie newSessionState(HttpServletRequest request, HttpServletResponse response) {
	return null;
	
}

private void replace(HttpServletRequest request, HttpServletResponse response, Cookie cookie) {
	// TODO Auto-generated method stub
	
}

private void refresh(HttpServletRequest request, HttpServletResponse response, Cookie cookie) {
	// TODO Auto-generated method stub
	
}

public static class SessionData {
	public String sessionID;
	public int version;
	public String message;
	public Date expiration_timestamp;
	
	public String packageCookie() {
		String value = "";
		value += this.sessionID;
		value += ",";
		value += String.valueOf(this.version);
		return value;
	}
	
	public static String getSessionID(String cookie) {
		return cookie.split(",")[0];
	}
}
}
