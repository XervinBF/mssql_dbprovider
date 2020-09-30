package org.xbf.addons.database.mssql;

import org.xbf.core.XBF;
import org.xbf.core.Exceptions.AnnotationNotPresent;
import org.xbf.core.Plugins.DependsOn;
import org.xbf.core.Plugins.PluginVersion;
import org.xbf.core.Plugins.XPlugin;
import org.xbf.core.Plugins.XervinJavaPlugin;

@XPlugin(name="mssql", description="A MSSQL database provider", displayname="MSSQL Db Provider")
@PluginVersion(currentVersion = "1.0.0")
@DependsOn(pluginName="xbf", minimumVersion="0.0.7")
public class MSSQLDatabaseProviderPlugin extends XervinJavaPlugin{

	@Override
	public void register() {
		try {
			XBF.registerDatabaseProvider(MSSQLDatabaseProvider.class);
		} catch (AnnotationNotPresent e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		System.out.println("Please do not run this as a java app, this is a XBF database provider.");
	}
	
}
