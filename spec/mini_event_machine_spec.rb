require File.join(File.dirname(__FILE__),"mono_table_helper_methods")

describe MiniEventMachine do
  it "should work to queue some stuff" do
    array=[]
    MiniEventMachine.queue {array << "first"}
    MiniEventMachine.queue {array << "second"}
    MiniEventMachine.queue {array << "third"}
    MiniEventMachine.process_queue
    array.should == ["first", "second", "third"]
  end

  it "should work to queue async stuff" do
    array=[]
    async_task = Proc.new {array << "first"}
    post_task = Proc.new {array << "second"}
    MiniEventMachine.defer(async_task,post_task)
    MiniEventMachine.wait_for_all_tasks
    array.should == ["first","second"]
  end
end
