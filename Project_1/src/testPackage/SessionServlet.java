package testPackage; // Always use packages. Never use default package.

import java.io.*; 
import javax.servlet.*;
import javax.servlet.annotation.*;
import javax.servlet.http.*;

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
	
  @Override
  public void doGet(HttpServletRequest request,
                    HttpServletResponse response)
      throws ServletException, IOException {
	  
	  String cmd = request.getParameter("command");
	  if(cmd == null)
		  newSessionState(request, response);
	  else if(cmd == "replace")
		  replace(request, response);
	  else if(cmd == "refresh")
		  refresh(request, response);
	  else if(cmd == "logout")
		  logout(request, response); 
	 
  }

private void logout(HttpServletRequest request, HttpServletResponse response) {
	// TODO Auto-generated method stub
	
}

private void newSessionState(HttpServletRequest request,
		HttpServletResponse response) {
	// TODO Auto-generated method stub
	
}

private void replace(HttpServletRequest request, HttpServletResponse response) {
	// TODO Auto-generated method stub
	
}

private void refresh(HttpServletRequest request, HttpServletResponse response) {
	// TODO Auto-generated method stub
	
}
}
