logLevel := Level.Warn

resolvers += Resolver.url("bintray-sbt-plugins", url("http://dl.bintray.com/sbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

addSbtPlugin("de.heikoseeberger"                 % "sbt-header"       % "1.8.0")
addSbtPlugin("com.geirsson"                      % "sbt-scalafmt"     % "0.6.8")
addSbtPlugin("com.dwijnand"                      % "sbt-dynver"       % "1.2.0")
addSbtPlugin("com.lightbend.paradox"             % "sbt-paradox"      % "0.2.9")
addSbtPlugin("com.eed3si9n"                      % "sbt-unidoc"       % "0.4.0")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "1.0.0")
addSbtPlugin("com.eed3si9n"                      % "sbt-assembly"     % "0.14.4")