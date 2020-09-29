package org.xbf.addons.database.mssql;

import org.xbf.Xervin5.Xervin;
import org.xbf.Xervin5.Exceptions.AnnotationNotPresent;
import org.xbf.Xervin5.Plugins.PluginVersion;
import org.xbf.Xervin5.Plugins.XPlugin;
import org.xbf.Xervin5.Plugins.XervinJavaPlugin;

@XPlugin(name="mssql", description="A MSSQL database provider", displayname="MSSQL Db Provider")
@PluginVersion(currentVersion = "1.0.0")
public class MSSQLDatabaseProviderPlugin extends XervinJavaPlugin{

	@Override
	public void register() {
		try {
			Xervin.registerDatabaseProvider(MSSQLDatabaseProvider.class);
		} catch (AnnotationNotPresent e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		System.out.println("Please do not run this as a java app, this is a XBF5 database provider.");
	}
	
}
