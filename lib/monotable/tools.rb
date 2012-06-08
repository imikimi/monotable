def monotable_require(relative_path,modules)
  modules.each do |mod|
    require File.join(File.dirname(__FILE__),relative_path.to_s,mod)
  end
end

monotable_require :patches, %w{
  eventmachine
  string
}

monotable_require :tools, %w{
  cache
  file_handle
  tools
  logger
}
